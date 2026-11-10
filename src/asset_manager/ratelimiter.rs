use tokio::time::Duration;
use tracing::info;
// ============================================================================
// RATE LIMITER
// ============================================================================

/// Handles rate limiting across all concurrent tasks
pub struct RateLimiter {
    until: tokio::sync::Mutex<Option<tokio::time::Instant>>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            until: tokio::sync::Mutex::new(None),
        }
    }

    /// Sets a rate limit that all tasks must wait for. Never shortens a longer
    /// rate limit that is already active.
    pub async fn set_rate_limit(&self, duration_secs: u64) {
        let wake_time = tokio::time::Instant::now() + Duration::from_secs(duration_secs);
        let mut until = self.until.lock().await;
        if until.is_some_and(|current| current > wake_time) {
            return;
        }
        *until = Some(wake_time);
        info!(
            "Global rate limit set: all tasks sleeping {} seconds",
            duration_secs
        );
    }

    /// Waits if rate limit is currently active
    pub async fn wait_if_limited(&self) {
        loop {
            let until = *self.until.lock().await;
            if let Some(wake_time) = until
                && tokio::time::Instant::now() < wake_time
            {
                tokio::time::sleep_until(wake_time).await;
                continue;
            }
            break;
        }
    }
}
