use super::ratelimiter::RateLimiter;
use super::uploader::AssetUploader;
use crate::asset_manager::UploadTask;
use bytes::Bytes;
use roboat::RoboatError;
use roboat::assetdelivery::AssetBatchResponse;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::Duration;

const MAX_UPLOAD_RETRIES: usize = 5;

// ============================================================================
// Upload Task Management
// ============================================================================
#[derive(Clone)]
pub struct UploadContext {
    pub uploader: Arc<AssetUploader>,
    pub group_id: Option<u64>,
    pub semaphore: Arc<Semaphore>,
    pub rate_limiter: Arc<RateLimiter>,
    pub total_assets: usize,
}

impl UploadContext {
    pub fn new(
        uploader: Arc<AssetUploader>,
        group_id: Option<u64>,
        max_concurrent_tasks: u64,
        total_assets: usize,
    ) -> Self {
        Self {
            rate_limiter: Arc::clone(&uploader.rate_limiter),
            uploader,
            group_id,
            semaphore: Arc::new(Semaphore::new(max_concurrent_tasks as usize)),
            total_assets,
        }
    }

    /// Spawns all upload tasks for concurrent asset uploads
    pub fn spawn_upload_tasks(&self, asset_batch: Vec<AssetBatchResponse>) -> Vec<UploadTask> {
        asset_batch
            .into_iter()
            .enumerate()
            .filter_map(|(index, asset)| {
                let location = asset
                    .locations
                    .as_ref()
                    .and_then(|locs| locs.first())
                    .and_then(|loc| loc.location.as_ref())?
                    .to_string();

                // Extract asset type from the asset response
                let asset_type = asset.asset_type?;

                Some(self.spawn_single_upload_task(
                    index,
                    asset.request_id.clone(),
                    location,
                    asset_type,
                ))
            })
            .collect()
    }

    fn spawn_single_upload_task(
        &self,
        index: usize,
        request_id: Option<String>,
        location: String,
        asset_type: roboat::catalog::AssetType,
    ) -> UploadTask {
        let ctx = self.clone();

        tokio::spawn(async move {
            let _permit = ctx.semaphore.acquire().await.unwrap();

            let asset_bytes = ctx
                .uploader
                .file_bytes_from_url(location)
                .await
                .map_err(|e| (request_id.clone(), e))?;

            ctx.rate_limiter.wait_if_limited().await;

            let new_asset_id = ctx
                .upload_asset_with_retry(
                    asset_bytes,
                    asset_type,
                    index,
                    request_id.clone().unwrap_or_else(|| "unknown".to_string()),
                )
                .await
                .map_err(|e| (request_id.clone(), e))?;

            Ok((request_id, new_asset_id))
        })
    }

    /// Uploads asset with automatic retry logic for rate limits and server errors
    async fn upload_asset_with_retry(
        &self,
        asset_bytes: Bytes,
        asset_type: roboat::catalog::AssetType,
        index: usize,
        request_id: String,
    ) -> Result<String, RoboatError> {
        let mut last_error = None;

        for attempt in 1..=MAX_UPLOAD_RETRIES {
            match self
                .uploader
                .upload_asset(asset_bytes.clone(), self.group_id, asset_type)
                .await
            {
                Ok(new_asset_id) => {
                    println!(
                        "[{:?}] Success uploading {} - {}/{} ({} remaining)",
                        asset_type,
                        request_id,
                        index + 1,
                        self.total_assets,
                        self.total_assets - (index + 1)
                    );
                    return Ok(new_asset_id);
                }
                Err(e) => {
                    eprintln!(
                        "Upload attempt {}/{} failed for {:?} {}: {}",
                        attempt, MAX_UPLOAD_RETRIES, asset_type, request_id, e
                    );

                    if matches!(
                        e,
                        RoboatError::TooManyRequests | RoboatError::InternalServerError
                    ) {
                        let sleep_time = (attempt as u64) * 30;
                        self.rate_limiter.set_rate_limit(sleep_time).await;
                        self.rate_limiter.wait_if_limited().await;
                    }

                    last_error = Some(e);

                    if attempt < MAX_UPLOAD_RETRIES {
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                    } else {
                        eprintln!("[FAIL] Failed to upload {:?} {}", asset_type, request_id);
                    }
                }
            }
        }

        Err(last_error.unwrap())
    }
}

/// Collects results from all upload tasks
pub async fn collect_upload_results(
    tasks: Vec<UploadTask>,
) -> Result<HashMap<String, String>, RoboatError> {
    let mut finished_asset_hashmap = HashMap::new();
    let mut errors = Vec::new();
    let total_tasks = tasks.len();

    for task in tasks {
        match task.await {
            Ok(Ok((Some(request_id), new_asset_id))) => {
                finished_asset_hashmap.insert(request_id, new_asset_id);
            }
            Ok(Ok((None, _))) => {
                eprintln!("Warning: Upload succeeded but no request_id available");
            }
            Ok(Err((request_id, e))) => {
                if matches!(e, RoboatError::BadRequest) {
                    eprintln!(
                        "Upload API error for {:?}: Cookie may lack required permissions\n\
            For group uploads, ensure the cookie has ALL Asset and Experience permissions",
                        request_id
                    );
                }
                errors.push(e);
            }
            Err(join_error) => {
                eprintln!("Task execution failed: {}", join_error);
            }
        }
    }

    eprintln!(
        "Upload summary: {} failed out of {} total tasks",
        errors.len(),
        total_tasks
    );

    Ok(finished_asset_hashmap)
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================
