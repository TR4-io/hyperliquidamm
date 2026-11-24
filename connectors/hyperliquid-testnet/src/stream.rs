use crate::{btc_touch, unix_time_ms, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::time::{timeout, timeout_at, Instant as Deadline};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";
const WAIT: Duration = Duration::from_secs(30);

pub fn start(samples: Option<NonZeroUsize>, bbo: bool) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(read_books(URL, samples, bbo, io::stdout().lock()))
}

struct Top<'a> {
    bid: Option<&'a str>,
    ask: Option<&'a str>,
    time: i64,
}

fn bbo_top(data: &Value) -> Result<Top<'_>> {
    if data["coin"] != "BTC" { return Err("Unexpected BBO instrument".into()); }
    let time = data["time"].as_i64().filter(|t| *t >= 0).ok_or("Invalid BBO timestamp")?;
    let pair = data["bbo"].as_array().ok_or("Missing BBO sides")?;
    let [bid, ask] = pair.as_slice() else { return Err("Expected two BBO sides".into()); };
    fn price(level: &Value) -> Result<Option<&str>> {
        if level.is_null() { return Ok(None); }
        Ok(Some(level["px"].as_str().ok_or("Missing BBO price")?))
    }
    Ok(Top { bid: price(bid)?, ask: price(ask)?, time })
}

// On equal timestamps prefer the event specifically describing the top of book.
fn advances(previous: Option<(i64, bool)>, time: i64, is_bbo: bool) -> bool {
    previous.is_none_or(|last| (time, is_bbo) > last)
}

async fn read_books(url: &str, samples: Option<NonZeroUsize>, bbo: bool, mut output: impl Write) -> Result<()> {
    let started = Instant::now();
    let (mut socket, _) = timeout(WAIT, connect_async(url)).await??;
    let channels: &[&str] = if bbo { &["l2Book", "bbo"] } else { &["l2Book"] };
    for channel in channels {
        let subscription = json!({"method":"subscribe","subscription":{"type":channel,"coin":"BTC"}});
        timeout(WAIT, socket.send(Message::Text(subscription.to_string().into()))).await??;
    }
    let mut previous = None;
    let mut count = 0;
    let mut deadline = Deadline::now() + WAIT;

    loop {
        let frame = timeout_at(deadline, socket.next()).await
            .map_err(|_| "No BTC book received for 30 seconds")?
            .ok_or("WebSocket ended before the next BTC book")??;
        let received_ms = unix_time_ms()?;
        let elapsed = started.elapsed();
        let text = match frame {
            Message::Text(text) => text,
            Message::Close(_) => return Err("Testnet closed the book stream".into()),
            Message::Ping(_) => {
                // Tungstenite queues the matching pong when it reads a ping.
                timeout_at(deadline, socket.flush()).await??;
                continue;
            }
            _ => continue,
        };
        let event: Value = serde_json::from_str(&text)?;
        let channel = event["channel"].as_str();
        let top = match channel {
            Some("l2Book") => {
                let (bid, ask, time) = btc_touch(&event["data"])?;
                Top { bid: Some(bid), ask: Some(ask), time }
            }
            Some("bbo") if bbo => bbo_top(&event["data"])?,
            Some("error") => return Err(format!("Testnet subscription error: {}", event["data"]).into()),
            Some("subscriptionResponse" | "pong") => continue,
            _ => return Err("Unexpected message on BTC book stream".into()),
        };
        let exchange_ms = top.time;
        let is_bbo = channel == Some("bbo");
        if bbo {
            if !advances(previous, exchange_ms, is_bbo) { continue; }
        } else if previous.is_some_and(|(last, _)| exchange_ms < last) {
            return Err("BTC book timestamp moved backwards".into());
        }
        count += 1;
        let mut row = json!({
            "network":"testnet", "coin":"BTC", "transport":"websocket",
            "bid":top.bid, "ask":top.ask, "source":channel, "exchangeTimeMs":exchange_ms,
            "responseReceivedMs":received_ms, "snapshotAgeAtReceiptMs":received_ms - exchange_ms,
            "initial":previous.is_none()
        });
        if previous.is_none() {
            row["timeToFirstBookMs"] = json!(elapsed.as_millis() as u64);
        }
        writeln!(output, "{row}")?;
        output.flush()?;
        if samples.is_some_and(|n| count == n.get()) {
            timeout(WAIT, socket.close(None)).await??;
            return Ok(());
        }
        previous = Some((exchange_ms, is_bbo));
        deadline = Deadline::now() + WAIT;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[test]
    fn bbo_clears_an_absent_side_and_rejects_malformed_levels() {
        let data = json!({"coin":"BTC","time":12,"bbo":[null,{"px":"101"}]});
        let top = bbo_top(&data).unwrap();
        assert_eq!((top.bid, top.ask, top.time), (None, Some("101"), 12));
        assert!(bbo_top(&json!({"coin":"BTC","time":12,"bbo":[{},null]})).is_err());
    }

    #[test]
    fn late_snapshots_and_duplicate_timestamps_cannot_replace_fresher_bbo() {
        assert!(advances(None, 10, false));
        assert!(advances(Some((10, false)), 10, true));
        assert!(!advances(Some((10, true)), 10, false));
        assert!(!advances(Some((10, true)), 10, true));
        assert!(!advances(Some((10, false)), 9, true));
        assert!(advances(Some((10, true)), 11, false));
    }

    #[test]
    fn one_subscription_streams_two_timestamped_books() {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(tcp).await.unwrap();
                let request = ws.next().await.unwrap().unwrap().into_text().unwrap();
                assert_eq!(serde_json::from_str::<Value>(&request).unwrap(),
                           json!({"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}));
                ws.send(Message::Text(json!({"channel":"subscriptionResponse"}).to_string().into())).await.unwrap();
                for t in [1000, 1400] {
                    let update = json!({"channel":"l2Book","data":{"coin":"BTC","time":t,
                                        "levels":[[{"px":"99"}],[{"px":"101"}]]}});
                    ws.send(Message::Text(update.to_string().into())).await.unwrap();
                }
                assert!(matches!(ws.next().await.unwrap().unwrap(), Message::Close(_)));
            });
            let mut bytes = Vec::new();
            read_books(&url, NonZeroUsize::new(2), false, &mut bytes).await.unwrap();
            server.await.unwrap();
            let rows: Vec<Value> = std::str::from_utf8(&bytes).unwrap().lines()
                .map(|line| serde_json::from_str(line).unwrap()).collect();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0]["initial"], true);
            assert!(rows[0]["timeToFirstBookMs"].is_u64());
            assert_eq!(rows[1]["initial"], false);
            assert_eq!(rows[1]["snapshotAgeAtReceiptMs"], rows[1]["responseReceivedMs"].as_i64().unwrap() - 1400);
        });
    }
}
