# Hyperliquid BTC testnet connector in Rust

This Cargo project connects to Hyperliquid testnet over HTTPS. Its default VS Code launch reads BTC perpetual metadata and the top of the order book. Public reads and order preparation work without a wallet, balance, or archive data.

## Run in VS Code

Open this folder (the folder containing `Cargo.toml`) in VS Code. Install the current stable Rust toolchain with Cargo. In the integrated terminal run:

```sh
cargo test
cargo run -- connect
```

A successful connection prints the network, BTC asset index, size precision, best bid and ask, and exchange timestamp in milliseconds. The output also includes local Unix timestamps immediately before the book request and after the full response body arrives. `bookRequestRttMs` uses a monotonic clock for that request alone. `snapshotAgeAtReceiptMs` subtracts the exchange timestamp from the local receipt timestamp. Clock skew affects that signed difference and can make it negative. Both measures exclude metadata lookup and JSON decoding. For F5 debugging, install the CodeLLDB extension and select **Read BTC testnet book**.

## Manual order lifecycle

| Command after `cargo run --` | Effect |
| --- | --- |
| `connect` | Reads the current BTC perpetual market from testnet. |
| `open-orders <account>` | Lists a public account’s open orders across markets. Supply the trading account address. |
| `prepare-order --side <buy-or-sell> --price <price> --size <BTC> --tif <alo-gtc-or-ioc> --reduce-only <true-or-false>` | Prints one unsigned BTC limit-order action. Optional `--client-id` accepts a 16-byte hex identifier, starting with `0x`. |
| `prepare-cancel <order-id>` | Prints an unsigned BTC cancellation for an exchange order ID. |
| `submit` | Reads one externally signed testnet envelope from standard input, validates its action against current BTC metadata and sends it once. |
| `status <account> <order-id-or-client-id>` | Reads the exchange state of the order. |

Replace the angle-bracket placeholders with your own inputs. Hyperliquid uses `Alo` for a post-only order, `Gtc` for good till canceled and `Ioc` for immediate or cancel. The CLI accepts those choices in lowercase. Price and size remain decimal strings. The CLI reads venue precision from metadata.

### Where signing fits

Preparation returns the exchange action in signing field order. An external Hyperliquid-compatible signer must sign that action for the **testnet L1 action domain** using the direct account or an authorized testnet agent, with a fresh millisecond nonce. Preserve action key order and decimal strings from preparation. The exchange expects the action, the same nonce and an ECDSA signature with fields `r`, `s`, and `v`. When using an expiry, sign and submit the same `expiresAfter` value. This connector accepts direct-account actions. Signing an ordinary text message produces a different signature. The official Rust SDK implements the exchange signing protocol. [Official Rust SDK](https://github.com/hyperliquid-dex/hyperliquid-rust-sdk) · [Signing documentation](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/signing)

For an intentionally authorized testnet submission, pipe the signer's complete JSON envelope into `cargo run -- submit`. The program forwards the original bytes to `https://api.hyperliquid-testnet.xyz/exchange`. A resting or filled acknowledgement includes the exchange order ID. A cancellation requires its own signed action. If the response is lost, use the account and client order ID to check status before considering another submission. A prepared order without a client ID can also be investigated using open orders and account history.

The tests use a loopback HTTP server and synthetic acknowledgements. A passing fixture establishes the client behavior. An exchange-confirmed execution additionally requires a valid authorized testnet signature and sufficient testnet collateral.

[Exchange endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint) · [Info endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
