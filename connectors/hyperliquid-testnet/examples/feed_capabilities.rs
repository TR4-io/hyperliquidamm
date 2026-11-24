//! Inspect the public testnet response shapes without an account.
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::{timeout_at, Instant};
use tokio_tungstenite::{connect_async_with_config, tungstenite::Message};

async fn inspect(label: &str, request: Value) -> Result<(), Box<dyn std::error::Error>> {
    let end = Instant::now() + Duration::from_secs(12);
    let (mut socket, _) = timeout_at(end, connect_async_with_config(
        "wss://api.hyperliquid-testnet.xyz/ws", None, true,
    )).await??;
    socket.send(Message::Text(request.to_string().into())).await?;
    let mut seen = std::collections::BTreeSet::new();
    while let Ok(Some(frame)) = timeout_at(end, socket.next()).await {
        if let Message::Text(text) = frame? {
            let msg: Value = serde_json::from_str(&text)?;
            let channel = msg["channel"].as_str().unwrap_or("?");
            let data = &msg["data"];
            let keys = data.as_object().map(|o| o.keys().cloned().collect::<Vec<_>>());
            if seen.insert(format!("{channel} {keys:?}")) {
                let summary = if channel == "error" { data.to_string() }
                    else if channel == "post" { format!("reply={} payload={} exchange={}", data["response"]["type"],
                        data["response"]["payload"]["type"], data["response"]["payload"]["data"]["time"]) }
                    else { format!("keys={keys:?}, exchange={}", data["s"]["time"]) };
                println!("{label} {channel} {summary}");
            }
            if channel == "post" || channel == "error" { break; }
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
        inspect("snapshot", json!({"method":"post","id":1,"request":{"type":"info",
            "payload":{"type":"l2Book","coin":"BTC"}}})).await?;
        inspect("incremental", json!({"method":"subscribe","subscription":{"type":"l2","c":"BTC"}})).await
    })
}
