use crate::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use flate2::read::DeflateDecoder;
use serde::Deserialize;
use serde_json::Value;
use std::io::Read;

const DEPTH: usize = 20;
const MAX_DIFF_BYTES: usize = 64 * 1024;

#[derive(Deserialize)]
struct Snapshot { coin: String, time: i64, levels: [Vec<SnapshotLevel>; 2] }
#[derive(Deserialize)]
struct SnapshotLevel { px: String, sz: String }
#[derive(Deserialize)]
struct Diff { c: String, t: i64, l: [Vec<Change>; 2], r: [Vec<usize>; 2] }
#[derive(Deserialize)]
struct Change { p: String, s: String }

struct Level {
    price: String,
    // All perp prices have at most six fractional digits. Compare exact millionths.
    key: u64,
}

impl Level {
    fn new(price: String) -> Result<Self> {
        let (whole, frac) = price.split_once('.').unwrap_or((&price, ""));
        let frac = frac.trim_end_matches('0');
        if whole.is_empty() || !whole.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) || frac.len() > 6 {
            return Err("Invalid incremental price".into());
        }
        let fractional = if frac.is_empty() { 0 } else { frac.parse::<u64>()? * 10u64.pow(6 - frac.len() as u32) };
        let key = whole.parse::<u64>()?.checked_mul(1_000_000)
            .and_then(|v| v.checked_add(fractional)).filter(|&v| v > 0).ok_or("Invalid price magnitude")?;
        Ok(Self { price, key })
    }
}

fn has_size(s: &str) -> Result<bool> {
    let size: f64 = s.parse()?;
    if !size.is_finite() || size < 0.0 { return Err("Invalid incremental size".into()); }
    Ok(size > 0.0)
}

fn apply_side(side: &mut Vec<Level>, removed: Vec<usize>, changes: Vec<Change>, bid: bool) -> Result<()> {
    let mut mask = [false; DEPTH];
    for i in removed {
        if i >= side.len() || mask[i] { return Err("Invalid previous-book removal index".into()); }
        mask[i] = true;
    }
    let mut original_index = 0;
    side.retain(|_| {
        let keep = !mask[original_index];
        original_index += 1;
        keep
    });
    for change in changes {
        let level = Level::new(change.p)?;
        let index = side.iter().position(|old| old.key == level.key);
        match (index, has_size(&change.s)?) {
            (Some(i), true) => side[i] = level,
            (Some(i), false) => { side.remove(i); }
            (None, true) => side.push(level),
            (None, false) => (),
        }
    }
    side.sort_unstable_by(|a, b| if bid { b.key.cmp(&a.key) } else { a.key.cmp(&b.key) });
    side.truncate(DEPTH);
    Ok(())
}

fn compressed_diff(data: &Value) -> Result<Diff> {
    let text = data.as_str().ok_or("Invalid compressed L2 payload")?;
    if text.len() > 2 * MAX_DIFF_BYTES { return Err("Oversized compressed L2 diff".into()); }
    let compressed = STANDARD.decode(text)?;
    let mut bytes = Vec::new();
    DeflateDecoder::new(&compressed[..]).take(MAX_DIFF_BYTES as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > MAX_DIFF_BYTES { return Err("Oversized decoded L2 diff".into()); }
    Ok(serde_json::from_slice(&bytes)?)
}

#[derive(Default)]
pub struct Incremental {
    sides: [Vec<Level>; 2],
    time: Option<i64>,
}

impl Incremental {
    pub fn touch(&self) -> (Option<&str>, Option<&str>) {
        (self.sides[0].first().map(|l| l.price.as_str()), self.sides[1].first().map(|l| l.price.as_str()))
    }

    pub fn apply(&mut self, data: &Value) -> Result<i64> {
        let result = self.apply_packet(data);
        if result.is_err() { *self = Self::default(); }
        result
    }

    fn apply_packet(&mut self, data: &Value) -> Result<i64> {
        let time = if let Some(value) = data.get("s") {
            let book = Snapshot::deserialize(value)?;
            if book.coin != "BTC" { return Err("Unexpected incremental snapshot instrument".into()); }
            for (i, levels) in book.levels.into_iter().enumerate() {
                if levels.len() > DEPTH { return Err("Unexpected incremental book depth".into()); }
                let side = &mut self.sides[i];
                side.clear();
                for level in levels {
                    if !has_size(&level.sz)? { return Err("Empty level in incremental snapshot".into()); }
                    let level = Level::new(level.px)?;
                    if side.last().is_some_and(|last| if i == 0 { last.key <= level.key } else { last.key >= level.key }) {
                        return Err("Unsorted incremental snapshot".into());
                    }
                    side.push(level);
                }
            }
            book.time
        } else {
            if self.time.is_none() { return Err("Incremental diff without its starting snapshot".into()); }
            let diff = if let Some(raw) = data.get("u") { Diff::deserialize(raw)? }
                       else { compressed_diff(data.get("c").ok_or("Missing L2 diff")?)? };
            if diff.c != "BTC" { return Err("Unexpected incremental diff instrument".into()); }
            for (i, (removals, updates)) in diff.r.into_iter().zip(diff.l).enumerate() {
                apply_side(&mut self.sides[i], removals, updates, i == 0)?;
            }
            diff.t
        };
        if time < 0 || self.time.is_some_and(|t| time < t) { return Err("Incremental timestamp moved backwards".into()); }
        if let (Some(bid), Some(ask)) = (self.sides[0].first(), self.sides[1].first()) {
            if bid.key >= ask.key { return Err("Crossed incremental book".into()); }
        }
        self.time = Some(time);
        Ok(time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::DeflateEncoder, Compression};
    use serde_json::json;
    use std::io::Write;

    fn snapshot() -> Value {
        json!({"s":{"coin":"BTC","time":10,"levels":[
            [{"px":"100","sz":"2"},{"px":"99","sz":"1"},{"px":"98","sz":"3"}],
            [{"px":"101","sz":"4"},{"px":"102","sz":"2"}]]}})
    }

    #[test]
    fn previous_indices_and_decimal_price_replacements_produce_a_sorted_touch() {
        let mut b = Incremental::default();
        b.apply(&snapshot()).unwrap();
        let plain = json!({"u":{"c":"BTC","t":11,"r":[[0,2],[0]],"l":[
            [{"p":"99.00","s":"7"},{"p":"99.5","s":"1"}],[{"p":"103","s":"1"}]]}});
        assert_eq!(b.apply(&plain).unwrap(), 11);
        assert_eq!(b.touch(), (Some("99.5"), Some("102")));
        assert_eq!(b.sides[0].len(), 2);
        let mut zip = DeflateEncoder::new(Vec::new(), Compression::default());
        zip.write_all(br#"{"c":"BTC","t":12,"r":[[0,1],[0]],"l":[[],[]]}"#).unwrap();
        b.apply(&json!({"c":STANDARD.encode(zip.finish().unwrap())})).unwrap();
        assert_eq!(b.touch(), (None, Some("103")));
    }

    #[test]
    fn invalid_diff_discards_the_book_until_a_new_native_snapshot() {
        let good = json!({"u":{"c":"BTC","t":11,"l":[[],[]],"r":[[],[]]}});
        let mut b = Incremental::default();
        assert!(b.apply(&good).is_err());
        for bad in [
            json!({"u":{"c":"BTC","t":11,"l":[[],[]],"r":[[3],[]]}}),
            json!({"u":{"c":"BTC","t":9,"l":[[],[]],"r":[[],[]]}}),
            json!({"u":{"c":"BTC","t":11,"l":[[{"p":"105","s":"1"}],[]],"r":[[],[]]}}),
            json!({"c":"bad-deflate"}),
        ] {
            b.apply(&snapshot()).unwrap();
            assert!(b.apply(&bad).is_err());
            assert_eq!(b.touch(), (None,None));
            assert!(b.apply(&good).is_err());
        }
    }
}
