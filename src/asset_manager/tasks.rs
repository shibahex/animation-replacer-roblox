use super::is_retryable;
use super::ratelimiter::RateLimiter;
use super::uploader::AssetUploader;
use crate::asset_manager::UploadTask;
use bytes::Bytes;
use roboat::RoboatError;
use roboat::assetdelivery::AssetBatchResponse;
use roboat::bedev2::AssetCreator;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;
use tokio::time::Duration;

use tracing::{error, info, warn};
const MAX_UPLOAD_RETRIES: usize = 5;

// ============================================================================
// Upload Task Management
// ============================================================================
#[derive(Clone)]
pub struct UploadContext {
    pub uploader: Arc<AssetUploader>,
    pub creator: AssetCreator,
    pub semaphore: Arc<Semaphore>,
    pub rate_limiter: Arc<RateLimiter>,
    pub total_assets: usize,
    /// How many uploads finished (or failed), for the progress logs.
    pub completed: Arc<AtomicUsize>,
}

impl UploadContext {
    pub fn new(
        uploader: Arc<AssetUploader>,
        creator: AssetCreator,
        max_concurrent_tasks: u64,
        total_assets: usize,
    ) -> Self {
        Self {
            rate_limiter: Arc::clone(&uploader.rate_limiter),
            uploader,
            creator,
            semaphore: Arc::new(Semaphore::new(max_concurrent_tasks.max(1) as usize)),
            total_assets,
            completed: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Spawns all upload tasks for concurrent asset uploads
    pub fn spawn_upload_tasks(&self, asset_batch: Vec<AssetBatchResponse>) -> Vec<UploadTask> {
        asset_batch
            .into_iter()
            .filter_map(|asset| {
                let location = asset_location(&asset)?;

                // Extract asset type from the asset response
                let asset_type = asset.asset_type?;

                Some(self.spawn_single_upload_task(asset.request_id.clone(), location, asset_type))
            })
            .collect()
    }

    fn spawn_single_upload_task(
        &self,
        request_id: Option<String>,
        location: String,
        asset_type: roboat::catalog::AssetType,
    ) -> UploadTask {
        let ctx = self.clone();

        tokio::spawn(async move {
            let _permit = ctx
                .semaphore
                .acquire()
                .await
                .map_err(|_| (request_id.clone(), RoboatError::InternalServerError))?;
            let name = request_id.clone().unwrap_or_else(|| "unknown".to_string());

            let asset_bytes: Bytes = match ctx.uploader.file_bytes_from_url(location).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    ctx.log_progress(false, asset_type, &name);
                    error!("Failed to download {:?} {}: {}", asset_type, name, e);
                    return Err((request_id, e));
                }
            };

            let new_asset_id = ctx
                .upload_asset_with_retry(asset_bytes, asset_type, name)
                .await
                .map_err(|e| (request_id.clone(), e))?;

            Ok((request_id, new_asset_id))
        })
    }

    /// Uploads an asset, retrying the start of the upload on rate limits and server errors.
    ///
    /// Only starting the upload is retried. Once Roblox accepted it, waiting for the result is
    /// done once, so a failure there can never upload the same asset twice.
    pub(super) async fn upload_asset_with_retry(
        &self,
        asset_bytes: Bytes,
        asset_type: roboat::catalog::AssetType,
        name: String,
    ) -> Result<String, RoboatError> {
        let mut attempt = 0;

        let operation_path = loop {
            attempt += 1;
            self.rate_limiter.wait_if_limited().await;

            match self
                .uploader
                .start_upload(&asset_bytes, self.creator, asset_type)
                .await
            {
                Ok(path) => break path,
                Err(e) if is_retryable(&e) && attempt < MAX_UPLOAD_RETRIES => {
                    warn!(
                        "Upload attempt {}/{} failed for {:?} {}: {}",
                        attempt, MAX_UPLOAD_RETRIES, asset_type, name, e
                    );

                    if matches!(e, RoboatError::TooManyRequests) {
                        // Pause every task, uploads are rate limited per account
                        self.rate_limiter.set_rate_limit(15 * attempt as u64).await;
                    } else {
                        tokio::time::sleep(Duration::from_secs(2 * attempt as u64)).await;
                    }
                }
                Err(e) => {
                    self.log_progress(false, asset_type, &name);
                    error!("Failed to upload {:?} {}: {}", asset_type, name, e);
                    return Err(e);
                }
            }
        };

        match self
            .uploader
            .finish_upload(operation_path.clone(), asset_type)
            .await
        {
            Ok(new_asset_id) => {
                self.log_progress(true, asset_type, &name);
                Ok(new_asset_id)
            }
            Err(e) => {
                self.log_progress(false, asset_type, &name);
                error!(
                    "{:?} {} was uploaded but its new id couldn't be read ({}): {}",
                    asset_type, name, operation_path, e
                );
                Err(e)
            }
        }
    }

    /// Logs that an upload finished, with how many are done out of the total.
    fn log_progress(&self, success: bool, asset_type: roboat::catalog::AssetType, name: &str) {
        let done = self.completed.fetch_add(1, Ordering::Relaxed) + 1;
        let status = if success { "Uploaded" } else { "Failed" };
        info!(
            "[{}/{}] {} {:?} {}",
            done, self.total_assets, status, asset_type, name
        );
    }
}

/// Gets the download url of an asset from its batch response.
pub fn asset_location(asset: &AssetBatchResponse) -> Option<String> {
    asset
        .locations
        .as_ref()
        .and_then(|locs| locs.first())
        .and_then(|loc| loc.location.clone())
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
                warn!("Upload succeeded but no request_id available");
            }
            Ok(Err((request_id, e))) => {
                if matches!(e, RoboatError::BadRequest) {
                    error!(
                        "Failed to upload asset: {:?} (cookie may lack required permissions)",
                        request_id
                    );
                }
                errors.push(e);
            }
            Err(join_error) => {
                error!("Task execution failed: {}", join_error);
            }
        }
    }

    info!(
        "Upload summary: {} failed out of {} total tasks",
        errors.len(),
        total_tasks
    );

    Ok(finished_asset_hashmap)
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================
