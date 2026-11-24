use crate::{incremental::Incremental, refresh::Refresh, unix_time_ms, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::time::{timeout, timeout_at, Instant};
use tokio_tungstenite::{connect_async_with_config, tungstenite::Message};

const URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";
const WAIT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode { L2, Bbo, Incremental, Fast }

pub fn start(samples: Option<NonZeroUsize>, mode: Mode) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(read_books(URL, samples, mode, io::stdout().lock()))
}

struct Top<'a> {
    bid: Option<&'a str>,
    ask: Option<&'a str>,
    time: i64,
}

fn price(level: &Value) -> Result<&str> {
    level["px"].as_str().ok_or_else(|| "Missing book price".into())
}

fn top(data: &Value, bbo: bool) -> Result<Top<'_>> {
    if data["coin"] != "BTC" { return Err("Unexpected book instrument".into()); }
    let time = data["time"].as_i64().filter(|t| *t >= 0).ok_or("Invalid book timestamp")?;
    let key = if bbo { "bbo" } else { "levels" };
    let sides = data[key].as_array().ok_or("Missing book sides")?;
    let [bid, ask] = sides.as_slice() else { return Err("Expected two book sides".into()); };
    fn side(level: &Value, bbo: bool) -> Result<Option<&str>> {
        if bbo {
            return if level.is_null() { Ok(None) } else { Ok(Some(price(level)?)) };
        }
        level.as_array().ok_or("Invalid book side")?.first().map(price).transpose()
    }
    Ok(Top { bid: side(bid, bbo)?, ask: side(ask, bbo)?, time })
}

// On equal timestamps prefer the event specifically describing the top of book.
fn advances(previous: Option<(i64, bool)>, time: i64, is_bbo: bool) -> bool {
    previous.is_none_or(|last| (time, is_bbo) > last)
}

async fn read_books(url: &str, samples: Option<NonZeroUsize>, mode: Mode, mut output: impl Write) -> Result<()> {
    let started = Instant::now();
    let (mut socket, _) = timeout(WAIT, connect_async_with_config(url, None, true)).await??;
    let bbo = mode != Mode::L2;
    let fast = mode == Mode::Fast;
    let diffs = matches!(mode, Mode::Incremental | Mode::Fast);
    let mut channels = vec![json!({"type":"l2Book","coin":"BTC"})];
    if bbo { channels.push(json!({"type":"bbo","coin":"BTC"})); }
    if diffs { channels.push(json!({"type":"l2","c":"BTC"})); }
    for channel in channels {
        let subscription = json!({"method":"subscribe","subscription":channel});
        timeout(WAIT, socket.feed(Message::Text(subscription.to_string().into()))).await??;
    }
    timeout(WAIT, socket.flush()).await??;
    let mut incremental = Incremental::default();
    let mut previous = None;
    let mut count = 0;
    let mut refresh = Refresh::new(fast, Instant::now());
    let mut deadline = Instant::now() + WAIT;

    loop {
        let now = Instant::now();
        if now >= deadline { return Err("No fresh BTC book received for 30 seconds".into()); }
        if let Some(request) = refresh.on_timer(now) {
            timeout_at(deadline, socket.send(Message::Text(request.to_string().into()))).await??;
        }
        let wake = refresh.next_wake().map_or(deadline, |t| t.min(deadline));
        let Ok(frame) = timeout_at(wake, socket.next()).await else { continue; };
        let frame = frame.ok_or("WebSocket ended before the next BTC book")??;
        let received = Instant::now();
        let received_ms = unix_time_ms()?;
        let text = match frame {
            Message::Text(text) => text,
            Message::Close(_) => return Err("Testnet closed the book stream".into()),
            Message::Ping(_) => {
                timeout_at(deadline, socket.flush()).await??;
                continue;
            }
            _ => continue,
        };
        let event: Value = serde_json::from_str(&text)?;
        let data = &event["data"];
        let mut request_rtt = None;
        let (book, source) = match event["channel"].as_str() {
            Some("l2Book") => (top(data, false)?, "l2Book"),
            Some("bbo") if bbo => (top(data, true)?, "bbo"),
            Some("l2") if diffs => {
                // Even an older diff must update its own book for later index-based removals.
                let time = incremental.apply(data)?;
                let (bid, ask) = incremental.touch();
                (Top { bid, ask, time }, "l2")
            }
            Some("post") if fast => {
                let Some(rtt) = refresh.response(data, received) else { continue; };
                let reply = &data["response"];
                if reply["type"] == "error" {
                    eprintln!("Book refresh rejected ({}). Backing off for 60 seconds.", reply["payload"]);
                    refresh.back_off(received);
                    continue;
                }
                if reply["type"] != "info" || reply["payload"]["type"] != "l2Book" {
                    return Err("Unexpected reply to public BTC book request".into());
                }
                request_rtt = Some(rtt);
                (top(&reply["payload"]["data"], false)?, "postL2Book")
            }
            Some("error") => return Err(format!("Testnet subscription error: {data}").into()),
            Some("subscriptionResponse" | "pong") => continue,
            _ => return Err("Unexpected message on BTC book stream".into()),
        };
        let exchange_ms = book.time;
        let is_bbo = source == "bbo";
        if bbo {
            if !advances(previous, exchange_ms, is_bbo) { continue; }
        } else if previous.is_some_and(|(last, _)| exchange_ms < last) {
            return Err("BTC book timestamp moved backwards".into());
        }
        if source != "postL2Book" { refresh.fresh_push(received); }
        let ready_ms = unix_time_ms()?;
        let mut row = json!({
            "network":"testnet", "coin":"BTC", "transport":"websocket",
            "bid":book.bid, "ask":book.ask, "source":source, "exchangeTimeMs":exchange_ms,
            "responseReceivedMs":received_ms, "snapshotAgeAtReceiptMs":received_ms - exchange_ms,
            "quoteReadyMs":ready_ms, "frameToQuoteUs":received.elapsed().as_micros() as u64,
            "initial":previous.is_none()
        });
        if previous.is_none() {
            row["timeToFirstBookMs"] = json!(received.duration_since(started).as_millis() as u64);
        }
        if fast { row["bookRequestsSent"] = json!(refresh.sent); }
        if let Some(rtt) = request_rtt { row["bookRequestRttMs"] = json!(rtt.as_millis() as u64); }
        writeln!(output, "{row}")?;
        output.flush()?;
        count += 1;
        if samples.is_some_and(|n| count == n.get()) {
            timeout(WAIT, socket.close(None)).await??;
            return Ok(());
        }
        previous = Some((exchange_ms, is_bbo));
        deadline = received + WAIT;
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
        let top = top(&data, true).unwrap();
        assert_eq!((top.bid, top.ask, top.time), (None, Some("101"), 12));
        assert!(super::top(&json!({"coin":"BTC","time":12,"bbo":[{},null]}), true).is_err());
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
            read_books(&url, NonZeroUsize::new(2), Mode::L2, &mut bytes).await.unwrap();
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

    #[test]
    fn interleaved_diffs_and_matching_refreshes_keep_the_newest_quote() {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(tcp).await.unwrap();
                for channel in [json!({"type":"l2Book","coin":"BTC"}), json!({"type":"bbo","coin":"BTC"}),
                                json!({"type":"l2","c":"BTC"})] {
                    let req = ws.next().await.unwrap().unwrap().into_text().unwrap();
                    assert_eq!(serde_json::from_str::<Value>(&req).unwrap()["subscription"], channel);
                }
                let request = ws.next().await.unwrap().unwrap().into_text().unwrap();
                assert_eq!(serde_json::from_str::<Value>(&request).unwrap()["id"], 1);
                let levels = json!([[{"px":"100","sz":"2"},{"px":"99","sz":"1"}],[{"px":"102","sz":"1"}]]);
                let post = |id: u64, t: i64| json!({"channel":"post","data":{"id":id,"response":{
                    "type":"info","payload":{"type":"l2Book","data":{"coin":"BTC","time":t,"levels":levels}}}}});
                let diff = |t: i64, rem: Vec<usize>| json!({"channel":"l2","data":{"u":{
                    "c":"BTC","t":t,"l":[[],[]],"r":[rem,[]]}}});
                for event in [
                    json!({"channel":"l2","data":{"s":{"coin":"BTC","time":100,"levels":levels}}}),
                    json!({"channel":"bbo","data":{"coin":"BTC","time":102,"bbo":[null,{"px":"102"}]}}),
                    post(7, 999), post(1, 99), diff(101, vec![0]), diff(104, vec![]),
                ] {
                    ws.send(Message::Text(event.to_string().into())).await.unwrap();
                }
                let next = ws.next().await.unwrap().unwrap().into_text().unwrap();
                assert_eq!(serde_json::from_str::<Value>(&next).unwrap()["id"], 2);
                ws.send(Message::Text(post(2, 105).to_string().into())).await.unwrap();
                assert!(matches!(ws.next().await.unwrap().unwrap(), Message::Close(_)));
            });
            let mut buf = Vec::new();
            read_books(&url, NonZeroUsize::new(4), Mode::Fast, &mut buf).await.unwrap();
            server.await.unwrap();
            let lines: Vec<Value> = std::str::from_utf8(&buf).unwrap().lines()
                .map(|l| serde_json::from_str(l).unwrap()).collect();
            assert_eq!(lines.iter().map(|r| r["exchangeTimeMs"].as_i64().unwrap()).collect::<Vec<_>>(),
                       vec![100,102,104,105]);
            assert_eq!(lines[1]["bid"], Value::Null);
            assert_eq!(lines[2]["bid"], "99");
            assert_eq!(lines[3]["source"], "postL2Book");
            assert!(lines[3]["bookRequestRttMs"].is_u64());
        });
    }
}
