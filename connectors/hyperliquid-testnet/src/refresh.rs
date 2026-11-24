use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::Instant;

const INTERVAL: Duration = Duration::from_millis(250);
const REPLY_TIMEOUT: Duration = Duration::from_secs(5);
const ERROR_BACKOFF: Duration = Duration::from_secs(60);

pub(super) struct Refresh {
    enabled: bool,
    next_send: Instant,
    pending: Option<(u64, Instant)>,
    pub sent: u64,
}

impl Refresh {
    pub fn new(enabled: bool, now: Instant) -> Self {
        Self { enabled, next_send: now, pending: None, sent: 0 }
    }

    pub fn next_wake(&self) -> Option<Instant> {
        self.enabled.then(|| self.pending.map_or(self.next_send, |(_, sent)| sent + REPLY_TIMEOUT))
    }

    pub fn on_timer(&mut self, now: Instant) -> Option<Value> {
        if !self.next_wake().is_some_and(|t| t <= now) { return None; }
        if self.pending.is_some() {
            // A missing reply leaves the remote state unknown. Keep this socket push-only.
            self.enabled = false;
            self.pending = None;
            eprintln!("Book refresh timed out. Continuing with subscribed feeds.");
            return None;
        }
        self.sent += 1;
        self.pending = Some((self.sent, now));
        self.next_send = now + INTERVAL;
        Some(json!({"method":"post","id":self.sent,"request":{"type":"info",
                    "payload":{"type":"l2Book","coin":"BTC"}}}))
    }

    pub fn fresh_push(&mut self, now: Instant) {
        self.next_send = self.next_send.max(now + INTERVAL);
    }

    // Only the matching ID can release the single in-flight request.
    pub fn response(&mut self, data: &Value, now: Instant) -> Option<Duration> {
        let (id, sent) = self.pending?;
        if data["id"].as_u64() != Some(id) { return None; }
        self.pending = None;
        Some(now.duration_since(sent))
    }

    pub fn back_off(&mut self, now: Instant) {
        self.next_send = now + ERROR_BACKOFF;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_is_paced_single_flight_and_backs_off_after_errors() {
        let now = Instant::now();
        let mut r = Refresh::new(true, now);
        assert_eq!(r.on_timer(now).unwrap()["request"]["type"], "info");
        assert!(r.on_timer(now + INTERVAL).is_none());
        assert!(r.response(&json!({"id":99}), now + INTERVAL).is_none());
        let reply = now + INTERVAL * 2;
        assert_eq!(r.response(&json!({"id":1}), reply), Some(INTERVAL * 2));
        r.fresh_push(reply);
        assert!(r.on_timer(reply).is_none());
        assert_eq!(r.on_timer(reply + INTERVAL).unwrap()["id"], 2);
        r.response(&json!({"id":2}), reply + INTERVAL);
        r.back_off(reply + INTERVAL);
        assert!(r.on_timer(reply + ERROR_BACKOFF).is_none());
        assert!(r.on_timer(reply + INTERVAL + ERROR_BACKOFF).is_some());
    }

    #[test]
    fn a_timed_out_request_cannot_trigger_a_retry_burst() {
        let now = Instant::now();
        let mut r = Refresh::new(true, now);
        assert!(r.on_timer(now).is_some());
        assert!(r.on_timer(now + REPLY_TIMEOUT).is_none());
        assert!(r.response(&json!({"id":1}), now + REPLY_TIMEOUT).is_none());
        assert_eq!(r.next_wake(), None);
    }
}
