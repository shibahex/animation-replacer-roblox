use bytes::Bytes;
use roboat::RoboatError;
use roboat::assetdelivery::AssetBatchResponse;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::Duration;

use super::uploader::AnimationUploader;
use crate::animation::UploadTask;

const MAX_UPLOAD_RETRIES: usize = 5;

// ============================================================================
// RATE LIMITER
// ============================================================================

/// Handles rate limiting across all concurrent tasks
pub struct RateLimiter {
    until: tokio::sync::Mutex<Option<tokio::time::Instant>>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            until: tokio::sync::Mutex::new(None),
        }
    }

    /// Sets a rate limit that all tasks must wait for
    pub async fn set_rate_limit(&self, duration_secs: u64) {
        let wake_time = tokio::time::Instant::now() + Duration::from_secs(duration_secs);
        *self.until.lock().await = Some(wake_time);
        println!(
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

    /// Clones the Arc for sharing across tasks
    pub fn clone_arc(self: &Arc<Self>) -> Arc<Self> {
        Arc::clone(self)
    }
}

// ============================================================================
// PUBLIC FUNCTIONS - Upload Task Management
// ============================================================================

/// Spawns all upload tasks for concurrent animation uploads
pub fn spawn_upload_tasks(
    uploader: Arc<AnimationUploader>,
    animations: Vec<AssetBatchResponse>,
    group_id: Option<u64>,
    max_concurrent_tasks: u64,
    total_animations: usize,
) -> Vec<UploadTask> {
    let semaphore = Arc::new(Semaphore::new(max_concurrent_tasks as usize));
    let roblosecurity = Arc::new(uploader.roblosecurity.clone());
    let rate_limiter = uploader.rate_limiter.clone_arc();

    animations
        .into_iter()
        .enumerate()
        .filter_map(|(index, animation)| {
            let location = animation
                .locations
                .as_ref()
                .and_then(|locs| locs.first())
                .and_then(|loc| loc.location.as_ref())?
                .to_string();

            Some(spawn_single_upload_task(
                roblosecurity.clone(),
                index,
                animation.request_id.clone(),
                location,
                group_id,
                semaphore.clone(),
                rate_limiter.clone(),
                total_animations,
            ))
        })
        .collect()
}

/// Collects results from all upload tasks
pub async fn collect_upload_results(
    tasks: Vec<UploadTask>,
) -> Result<HashMap<String, String>, RoboatError> {
    let mut animation_hashmap = HashMap::new();
    let mut errors = Vec::new();
    let total_tasks = tasks.len();

    for task in tasks {
        match task.await {
            Ok(Ok((Some(request_id), new_animation_id))) => {
                animation_hashmap.insert(request_id, new_animation_id);
            }
            Ok(Ok((None, _))) => {
                eprintln!("Warning: Upload succeeded but no request_id available");
            }
            Ok(Err(e)) => {
                if matches!(e, RoboatError::BadRequest) {
                    eprintln!(
                        "Upload API error: Cookie may lack required permissions\n\
                         For group uploads, ensure the cookie has ALL Asset and Experience permissions"
                    );
                }
                errors.push(e);
            }
            Err(join_error) => {
                eprintln!("Task execution failed: {}", join_error);
            }
        }
    }

    if !errors.is_empty() {
        eprintln!(
            "Upload summary: {} failed out of {} total tasks",
            errors.len(),
            total_tasks
        );
    }

    Ok(animation_hashmap)
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Spawns a single upload task with all necessary context
fn spawn_single_upload_task(
    roblosecurity: Arc<String>,
    index: usize,
    request_id: Option<String>,
    location: String,
    group_id: Option<u64>,
    semaphore: Arc<Semaphore>,
    rate_limiter: Arc<RateLimiter>,
    total_animations: usize,
) -> UploadTask {
    tokio::spawn(async move {
        // Acquire semaphore permit
        let _permit = semaphore.acquire().await.unwrap();

        // Create uploader instance
        let uploader = AnimationUploader::new((*roblosecurity).clone());

        // Download animation file
        let animation_file = uploader.file_bytes_from_url(location).await?;

        // Wait for rate limit if needed
        rate_limiter.wait_if_limited().await;

        // Upload with retry logic
        let new_animation_id = upload_animation_with_retry(
            &uploader,
            animation_file,
            group_id,
            &rate_limiter,
            index,
            total_animations,
            request_id.clone().unwrap_or_else(|| "unknown".to_string()),
        )
        .await?;

        Ok((request_id, new_animation_id))
    })
}

/// Uploads animation with automatic retry logic for rate limits and server errors
async fn upload_animation_with_retry(
    uploader: &AnimationUploader,
    animation_file: Bytes,
    group_id: Option<u64>,
    rate_limiter: &Arc<RateLimiter>,
    index: usize,
    total_animations: usize,
    request_id: String,
) -> Result<String, RoboatError> {
    let mut last_error = None;

    for attempt in 1..=MAX_UPLOAD_RETRIES {
        match uploader
            .upload_animation(animation_file.clone(), group_id)
            .await
        {
            Ok(new_animation_id) => {
                println!(
                    "Success uploading animation {} - {}/{} ({} remaining)",
                    request_id,
                    index + 1,
                    total_animations,
                    total_animations - (index + 1)
                );
                return Ok(new_animation_id);
            }
            Err(e) => {
                eprintln!(
                    "Upload attempt {}/{} failed for animation {}: {}",
                    attempt, MAX_UPLOAD_RETRIES, request_id, e
                );

                // Handle rate limits and server errors
                if matches!(
                    e,
                    RoboatError::TooManyRequests | RoboatError::InternalServerError
                ) {
                    let sleep_time = (attempt as u64) * 30;
                    rate_limiter.set_rate_limit(sleep_time).await;
                    rate_limiter.wait_if_limited().await;
                }

                last_error = Some(e);

                // Small delay between retries (except last attempt)
                if attempt < MAX_UPLOAD_RETRIES {
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        }
    }

    Err(last_error.unwrap())
}
