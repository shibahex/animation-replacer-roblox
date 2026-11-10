use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};
use tracing::info;
// ============================================================================
// RATE LIMITER
// ============================================================================

/// How far apart tasks start again after a rate limit ends. Without this, every task
/// retries at the same moment and gets rate limited again right away.
const RESUME_SPACING: Duration = Duration::from_millis(250);

/// Handles rate limiting across all concurrent tasks
pub struct RateLimiter {
    /// When the current rate limit ends.
    until: Mutex<Option<Instant>>,
    /// The next moment a task may start again after waiting out a rate limit.
    next_resume: Mutex<Instant>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            until: Mutex::new(None),
            next_resume: Mutex::new(Instant::now()),
        }
    }

    /// Sets a rate limit that all tasks must wait for. Never shortens a longer
    /// rate limit that is already active.
    pub async fn set_rate_limit(&self, duration_secs: u64) {
        let wake_time = Instant::now() + Duration::from_secs(duration_secs);
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

    /// Waits if rate limit is currently active. Tasks that had to wait start again one
    /// after another, spaced apart, instead of all at once.
    pub async fn wait_if_limited(&self) {
        let mut waited = false;

        loop {
            let until = *self.until.lock().await;
            if let Some(wake_time) = until
                && Instant::now() < wake_time
            {
                waited = true;
                tokio::time::sleep_until(wake_time).await;
                continue;
            }
            break;
        }

        if waited {
            let resume_at = {
                let mut next_resume = self.next_resume.lock().await;
                let resume_at = (*next_resume).max(Instant::now());
                *next_resume = resume_at + RESUME_SPACING;
                resume_at
            };
            tokio::time::sleep_until(resume_at).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RESUME_SPACING, RateLimiter};
    use std::sync::Arc;
    use tokio::time::Instant;

    #[tokio::test(start_paused = true)]
    async fn tasks_resume_one_after_another() {
        let limiter = Arc::new(RateLimiter::new());
        limiter.set_rate_limit(1).await;

        let start = Instant::now();
        let tasks: Vec<_> = (0..3)
            .map(|_| {
                let limiter = Arc::clone(&limiter);
                tokio::spawn(async move {
                    limiter.wait_if_limited().await;
                    Instant::now()
                })
            })
            .collect();

        let mut resumed = Vec::new();
        for task in tasks {
            resumed.push(task.await.unwrap() - start);
        }
        resumed.sort();

        assert!(resumed[0] >= std::time::Duration::from_secs(1));
        assert!(resumed[2] - resumed[0] >= RESUME_SPACING * 2);
    }

    #[tokio::test(start_paused = true)]
    async fn never_shortens_a_longer_limit() {
        let limiter = RateLimiter::new();
        limiter.set_rate_limit(30).await;
        limiter.set_rate_limit(5).await;

        let start = Instant::now();
        limiter.wait_if_limited().await;
        assert!(Instant::now() - start >= std::time::Duration::from_secs(30));
    }
}
