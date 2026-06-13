use bytes::Bytes;
use roboat::ClientBuilder;
use roboat::RoboatError;
use roboat::catalog::AssetType;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use super::ratelimiter::RateLimiter;
use super::tasks::{UploadContext, collect_upload_results};
const DEFAULT_CONCURRENT_TASKS: u64 = 50;

pub struct AssetUploader {
    pub roblosecurity: String,
    pub api_key: String,
    pub(super) rate_limiter: Arc<RateLimiter>,
}

impl AssetUploader {
    /// Creates a new AnimationUploader with a roblosecurity cookie.
    pub fn new(roblosecurity: String, api_key: String) -> Self {
        Self {
            roblosecurity,
            api_key,
            rate_limiter: Arc::new(RateLimiter::new()),
        }
    }

    /// Uploads a single animation to Roblox.
    pub async fn upload_asset(
        &self,
        asset_data: Bytes,
        group_id: Option<u64>,
        asset_type: AssetType,
    ) -> Result<String, RoboatError> {
        let client = ClientBuilder::new()
            .roblosecurity(self.roblosecurity.clone())
            .build();

        let resp = client
            .upload_animation(
                self.api_key.clone(),
                group_id,
                "reuploaded_animation".to_string(),
                "This is an example".to_string(),
                &asset_data,
            )
            .await?;

        loop {
            let operation = client
                .poll_upload_operation(self.api_key.clone(), resp.path.clone())
                .await?;

            if operation.done {
                if let Some(asset_id) = operation.response.and_then(|resp| resp.asset_id) {
                    info!("Uploaded complete! {:?}", asset_id);
                    return Ok(asset_id);
                }
                // TODO: Dont just return internal server error
                return Err(RoboatError::InternalServerError);
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    /// Reuploads multiple animations concurrently.
    pub async fn reupload_all_assets(
        self: Arc<Self>,
        assets: Vec<roboat::assetdelivery::AssetBatchResponse>,
        group_id: Option<u64>,
        task_count: Option<u64>,
    ) -> Result<HashMap<String, String>, RoboatError> {
        let max_concurrent_tasks = task_count.unwrap_or(DEFAULT_CONCURRENT_TASKS);
        let total_assets = assets.len();

        let ctx = UploadContext::new(self, group_id, max_concurrent_tasks, total_assets);

        let tasks = ctx.spawn_upload_tasks(assets);
        collect_upload_results(tasks).await
    }
}
