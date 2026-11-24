use clap::{Parser, Subcommand, ValueEnum};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{self, Read};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const BASE: &str = "https://api.hyperliquid-testnet.xyz";
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser)]
#[command(about = "Manual BTC perpetual connector for Hyperliquid testnet")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Read the BTC perpetual metadata and top of book.
    Connect,
    /// Read open orders for a public testnet account address.
    OpenOrders { account: String },
    /// Look up an order by exchange ID or 16-byte client order ID.
    Status { account: String, id: String },
    /// Print an unsigned BTC order action. Every order instruction is explicit.
    PrepareOrder {
        #[arg(long, value_enum)] side: Side,
        #[arg(long)] price: String,
        #[arg(long)] size: String,
        #[arg(long, value_enum)] tif: Tif,
        #[arg(long, action = clap::ArgAction::Set, required = true)] reduce_only: bool,
        #[arg(long)] client_id: Option<String>,
    },
    /// Print an unsigned cancel action for a BTC order ID.
    PrepareCancel { oid: u64 },
    /// Read one externally signed BTC order or cancel envelope from standard input.
    Submit,
}

#[derive(Clone, ValueEnum)]
enum Side { Buy, Sell }

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ValueEnum)]
enum Tif { Alo, Gtc, Ioc }

#[derive(Deserialize)]
struct Meta { universe: Vec<Asset> }
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Asset { name: String, sz_decimals: u32, #[serde(default)] is_delisted: bool }

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", deny_unknown_fields)]
enum Action {
    #[serde(rename = "order")]
    Order { orders: [Order; 1], grouping: String },
    #[serde(rename = "cancel")]
    Cancel { cancels: [Cancel; 1] },
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
struct Order {
    a: usize, b: bool, p: String, s: String, r: bool, t: LimitType,
    #[serde(skip_serializing_if = "Option::is_none")] c: Option<String>,
}
#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
struct LimitType { limit: Limit }
#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
struct Limit { tif: Tif }
#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
struct Cancel { a: usize, o: u64 }

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Envelope {
    action: Action, nonce: u64, signature: Signature,
    vault_address: Option<String>, expires_after: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Signature { r: String, s: String, v: u8 }

fn hex(s: &str, digits: usize) -> bool {
    s.strip_prefix("0x").is_some_and(|v| v.len() == digits && v.bytes().all(|b| b.is_ascii_hexdigit()))
}

fn decimal(s: &str) -> Result<(usize, usize)> {
    let (whole, frac) = s.split_once('.').unwrap_or((s, ""));
    if whole.is_empty() || s.ends_with('.') || !whole.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
        return Err("Expected a positive decimal in plain notation".into());
    }
    let trimmed = frac.trim_end_matches('0');
    let digits = format!("{whole}{trimmed}");
    let significant = digits.trim_start_matches('0').len();
    if significant == 0 { return Err("Price and size must be positive".into()); }
    Ok((trimmed.len(), significant))
}

fn canonical_decimal(value: &str) -> Result<String> {
    decimal(value)?;
    let (whole, frac) = value.split_once('.').unwrap_or((value, ""));
    let whole = whole.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    let frac = frac.trim_end_matches('0');
    Ok(if frac.is_empty() { whole.to_owned() } else { format!("{whole}.{frac}") })
}

impl Action {
    fn validate(&self, btc: usize, size_dp: u32) -> Result<()> {
        match self {
            Self::Order { orders: [order], grouping } => {
                if grouping != "na" || order.a != btc { return Err("Expected one ungrouped BTC order".into()); }
                let (dp, sig) = decimal(&order.p)?;
                let max_price_dp = 6u32.checked_sub(size_dp).ok_or("Invalid perpetual size precision")? as usize;
                if dp > max_price_dp || (dp > 0 && sig > 5) { return Err("Invalid BTC perpetual price precision".into()); }
                if decimal(&order.s)?.0 > size_dp as usize { return Err("Invalid BTC size precision".into()); }
                if order.c.as_ref().is_some_and(|c| !hex(c, 32)) { return Err("Invalid client order ID".into()); }
            }
            Self::Cancel { cancels: [c] } => {
                if c.a != btc || c.o == 0 { return Err("Expected a BTC order ID".into()); }
            }
        }
        Ok(())
    }
}

fn info(client: &Client, base: &str, request: Value) -> Result<Value> {
    Ok(client.post(format!("{base}/info")).json(&request).send()?.error_for_status()?.json()?)
}

fn unix_time_ms() -> Result<i64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis().try_into()?)
}

fn btc_meta(client: &Client, base: &str) -> Result<(usize, Asset)> {
    let meta: Meta = serde_json::from_value(info(client, base, json!({"type":"meta"}))?)?;
    meta.universe.into_iter().enumerate().find(|(_, a)| a.name == "BTC" && !a.is_delisted)
        .ok_or_else(|| "Active BTC perpetual missing from testnet metadata".into())
}

fn address(s: &str) -> Result<()> {
    if !hex(s, 40) { return Err("Expected a 20-byte account address".into()); }
    Ok(())
}

fn validate_envelope(envelope: &Envelope, btc: usize, precision: u32) -> Result<()> {
    envelope.action.validate(btc, precision)?;
    if envelope.vault_address.is_some() || envelope.nonce == 0 || envelope.expires_after == Some(0) {
        return Err("Expected a direct-account envelope with a nonzero nonce".into());
    }
    for word in [&envelope.signature.r, &envelope.signature.s] {
        let raw = word.strip_prefix("0x").ok_or("Invalid signature hex")?;
        if raw.is_empty() || raw.len() > 64 || !raw.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err("Invalid signature scalar".into());
        }
    }
    if !matches!(envelope.signature.v, 27 | 28) { return Err("Invalid recovery ID".into()); }
    Ok(())
}

fn acknowledgement(reply: Value, action: &Action) -> Result<Value> {
    if reply["status"] != "ok" { return Err(format!("Exchange returned {reply}").into()); }
    let expected = match action { Action::Order { .. } => "order", Action::Cancel { .. } => "cancel" };
    if reply["response"]["type"] != expected { return Err("Unexpected exchange response type. Check order status.".into()); }
    let statuses = reply["response"]["data"]["statuses"].as_array().ok_or("Missing order acknowledgement. Check order status.")?;
    if statuses.len() != 1 { return Err("Unexpected acknowledgement count. Check order status.".into()); }
    let s = &statuses[0];
    if let Some(error) = s.get("error") { return Err(format!("Exchange rejected action: {error}").into()); }
    let confirmed = match action {
        Action::Order { .. } => ["resting","filled"].iter().any(|k| s[*k]["oid"].as_u64().is_some()),
        Action::Cancel { .. } => s == "success",
    };
    if !confirmed { return Err("Unrecognized acknowledgement. Check order status.".into()); }
    Ok(reply)
}

fn run(client: &Client, base: &str, command: Command, input: impl Read) -> Result<String> {
    match command {
        Command::OpenOrders { account } => {
            address(&account)?;
            Ok(info(client, base, json!({"type":"openOrders","user":account}))?.to_string())
        }
        Command::Status { account, id } => {
            address(&account)?;
            let oid = if hex(&id, 32) { json!(id) } else { json!(id.parse::<u64>()?) };
            Ok(info(client, base, json!({"type":"orderStatus","user":account,"oid":oid}))?.to_string())
        }
        Command::Connect => {
            let (asset, meta) = btc_meta(client, base)?;
            let request = client.post(format!("{base}/info")).json(&json!({"type":"l2Book","coin":"BTC"})).build()?;
            let sent_ms = unix_time_ms()?;
            let start = Instant::now();
            let body = client.execute(request)?.error_for_status()?.bytes()?;
            let elapsed = start.elapsed();
            let received_ms = unix_time_ms()?;
            let book: Value = serde_json::from_slice(&body)?;
            if book["coin"] != "BTC" { return Err("Unexpected book instrument".into()); }
            let bid = book.pointer("/levels/0/0/px").and_then(Value::as_str).ok_or("Missing bid")?;
            let ask = book.pointer("/levels/1/0/px").and_then(Value::as_str).ok_or("Missing ask")?;
            let time = book["time"].as_i64().filter(|t| *t >= 0).ok_or("Invalid exchange timestamp")?;
            Ok(json!({"network":"testnet","coin":"BTC","asset":asset,"sizeDecimals":meta.sz_decimals,
                      "bid":bid,"ask":ask,"exchangeTimeMs":time,
                      "requestSentMs":sent_ms,"responseReceivedMs":received_ms,
                      "bookRequestRttMs":elapsed.as_millis() as u64,
                      "snapshotAgeAtReceiptMs":received_ms - time}).to_string())
        }
        Command::PrepareOrder { side, price, size, tif, reduce_only, client_id } => {
            let (a, meta) = btc_meta(client, base)?;
            let action = Action::Order { orders: [Order { a, b: matches!(side, Side::Buy), p: canonical_decimal(&price)?,
                s: canonical_decimal(&size)?, r: reduce_only, t: LimitType { limit: Limit { tif } }, c: client_id }],
                grouping: "na".into() };
            action.validate(a, meta.sz_decimals)?;
            // Keep struct field order for the external MessagePack signer.
            Ok(serde_json::to_string(&action)?)
        }
        Command::PrepareCancel { oid } => {
            let (a, meta) = btc_meta(client, base)?;
            let action = Action::Cancel { cancels: [Cancel { a, o: oid }] };
            action.validate(a, meta.sz_decimals)?;
            Ok(serde_json::to_string(&action)?)
        }
        Command::Submit => {
            let mut raw = Vec::new();
            input.take(65537).read_to_end(&mut raw)?;
            if raw.len() > 65536 { return Err("Envelope exceeds 64 KiB".into()); }
            let envelope: Envelope = serde_json::from_slice(&raw)?;
            let (a, meta) = btc_meta(client, base)?;
            validate_envelope(&envelope, a, meta.sz_decimals)?;
            // Forward the original bytes so signed field order and decimals survive validation.
            let reply = client.post(format!("{base}/exchange")).header("Content-Type","application/json")
                .body(raw).send().and_then(|r| r.error_for_status()).and_then(|r| r.json())
                .map_err(|e| format!("Order state unconfirmed. Query status before retrying. {e}"))?;
            Ok(acknowledgement(reply, &envelope.action)?.to_string())
        }
    }
}

fn main() {
    let command = Cli::parse().command;
    let result = Client::builder().retry(reqwest::retry::never()).timeout(Duration::from_secs(20)).redirect(reqwest::redirect::Policy::none())
        .build().map_err(Into::into).and_then(|client| run(&client, BASE, command, io::stdin().lock()));
    match result {
        Ok(output) => println!("{output}"),
        Err(e) => { eprintln!("{e}"); std::process::exit(1); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;

    const META: &str = r#"{"universe":[{"name":"ETH","szDecimals":4},{"name":"BTC","szDecimals":5}]}"#;

    fn client() -> Client {
        Client::builder().no_proxy().retry(reqwest::retry::never())
            .timeout(Duration::from_secs(3)).build().unwrap()
    }

    #[test]
    fn connect_reads_metadata_then_book() {
        let mut server = Server::new();
        let meta = server.mock("POST","/info").match_body(r#"{"type":"meta"}"#)
            .with_body(META).create();
        let book = server.mock("POST","/info")
            .match_body(mockito::Matcher::Json(json!({"type":"l2Book","coin":"BTC"})))
            .with_body(r#"{"coin":"BTC","time":1234,"levels":[[{"px":"99"}],[{"px":"100"}]]}"#).create();
        let data = run(&client(), &server.url(), Command::Connect, io::empty()).unwrap();
        let response: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(response["asset"],1);
        assert_eq!(response["bid"],"99");
        assert!(response["requestSentMs"].is_i64());
        assert!(response["bookRequestRttMs"].is_u64());
        let received = response["responseReceivedMs"].as_i64().unwrap();
        assert_eq!(response["snapshotAgeAtReceiptMs"], received - 1234);
        meta.assert(); book.assert();
    }

    #[test]
    fn prepare_requires_instructions_and_only_reads_info() {
        let mut server = Server::new();
        let meta = server.mock("POST","/info").with_body(META).create();
        let exchange = server.mock("POST","/exchange").expect(0).create();
        assert!(Cli::try_parse_from(["app","prepare-order"]).is_err());
        let command = Cli::try_parse_from(["app","prepare-order","--side","sell","--price","100","--size","0.01",
            "--reduce-only","false","--tif","alo"]).unwrap().command;
        let output=run(&client(),&server.url(),command,io::empty()).unwrap();
        assert_eq!(output,r#"{"type":"order","orders":[{"a":1,"b":false,"p":"100","s":"0.01","r":false,"t":{"limit":{"tif":"Alo"}}}],"grouping":"na"}"#);
        meta.assert(); exchange.assert();
    }

    #[test]
    fn submit_preserves_signed_bytes_and_forwards_only_once() {
        let mut server = Server::new();
        let raw = r#"{ "action":{"type":"cancel","cancels":[{"a":1,"o":7}]},"signature":{"r":"0x1","s":"0x2","v":27},"nonce":12 }"#;
        let meta = server.mock("POST","/info").with_body(META).create();
        let exchange = server.mock("POST","/exchange").match_body(raw).expect(1)
            .with_body(r#"{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}"#).create();
        run(&client(),&server.url(),Command::Submit,raw.as_bytes()).unwrap();
        meta.assert(); exchange.assert();
    }

    #[test]
    fn ambiguous_submission_can_be_reconciled_without_resending_the_order() {
        let mut server = Server::new();
        let account = "0x0000000000000000000000000000000000000001";
        let client_id = "0x00000000000000000000000000000007";
        let raw = json!({
            "action": {"type":"order","orders":[{
                "a":1,"b":true,"p":"100","s":"0.01","r":false,
                "t":{"limit":{"tif":"Gtc"}},"c":client_id
            }],"grouping":"na"},
            "nonce":12,"signature":{"r":"0x1","s":"0x2","v":27}
        }).to_string();
        let meta = server.mock("POST", "/info")
            .match_body(mockito::Matcher::Json(json!({"type":"meta"})))
            .with_body(META).create();
        let send = server.mock("POST", "/exchange")
            .match_body(mockito::Matcher::Exact(raw.clone()))
            .with_status(503).expect(1).create();
        let status = server.mock("POST", "/info")
            .match_body(mockito::Matcher::Json(json!({
                "type":"orderStatus","user":account,"oid":client_id
            })))
            .with_body(r#"{"status":"order","order":{"status":"open","order":{"oid":7}}}"#)
            .create();
        let http = client();
        let err = run(&http, &server.url(), Command::Submit, raw.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("Order state unconfirmed"));
        let query = Command::Status { account: account.into(), id: client_id.into() };
        let output = run(&http, &server.url(), query, io::empty()).unwrap();
        let result: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(result["order"]["order"]["oid"], 7);
        meta.assert();
        send.assert();
        status.assert();
    }

    #[test]
    fn rejects_invalid_precision_mismatched_asset_and_unknown_actions() {
        let action = |p:&str,s:&str| Action::Order { orders:[Order {a:1,b:true,p:p.into(),s:s.into(),r:false,
            t:LimitType {limit:Limit {tif:Tif::Gtc}},c:None}], grouping:"na".into() };
        assert_eq!(canonical_decimal("00001.0100").unwrap(), "1.01");
        assert!(action("100000","0.001").validate(1,5).is_ok());
        for (px,sz) in [("0","1"),("12345.1","1"),("1.01","1"),("1","0.000001"),("NaN","1")] {
            assert!(action(px,sz).validate(1,5).is_err());
        }
        assert!(action("1","1").validate(0,5).is_err());
        assert!(serde_json::from_value::<Action>(json!({"type":"withdraw3"})).is_err());
    }

    #[test]
    fn distinguishes_exchange_acceptance_from_http_success() {
        let order: Action = serde_json::from_str(r#"{"type":"order","orders":[{"a":1,"b":true,"p":"12","s":"1","r":false,"t":{"limit":{"tif":"Gtc"}}}],"grouping":"na"}"#).unwrap();
        let reply = |status:Value| json!({"status":"ok","response":{"type":"order","data":{"statuses":[status]}}});
        assert!(acknowledgement(reply(json!({"resting":{"oid":1}})),&order).is_ok());
        assert!(acknowledgement(reply(json!({"filled":{"oid":1}})),&order).is_ok());
        assert!(acknowledgement(reply(json!({"error":"rejected"})),&order).is_err());
        assert!(acknowledgement(reply(json!("success")),&order).is_err());
    }
}
