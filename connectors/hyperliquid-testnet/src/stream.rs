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

pub fn start(samples: Option<NonZeroUsize>) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(read_books(URL, samples, io::stdout().lock()))
}

async fn read_books(url: &str, samples: Option<NonZeroUsize>, mut output: impl Write) -> Result<()> {
    let started = Instant::now();
    let (mut socket, _) = timeout(WAIT, connect_async(url)).await??;
    let subscription = json!({"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}});
    timeout(WAIT, socket.send(Message::Text(subscription.to_string().into()))).await??;
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
        match event["channel"].as_str() {
            Some("l2Book") => (),
            Some("error") => return Err(format!("Testnet subscription error: {}", event["data"]).into()),
            Some("subscriptionResponse" | "pong") => continue,
            _ => return Err("Unexpected message on BTC book stream".into()),
        }
        let (bid, ask, exchange_ms) = btc_touch(&event["data"])?;
        if previous.is_some_and(|last| exchange_ms < last) {
            return Err("BTC book timestamp moved backwards".into());
        }
        count += 1;
        let mut row = json!({
            "network":"testnet", "coin":"BTC", "transport":"websocket",
            "bid":bid, "ask":ask, "exchangeTimeMs":exchange_ms,
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
        previous = Some(exchange_ms);
        deadline = Deadline::now() + WAIT;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

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
            read_books(&url, NonZeroUsize::new(2), &mut bytes).await.unwrap();
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
