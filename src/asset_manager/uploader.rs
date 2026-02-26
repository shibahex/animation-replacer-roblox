use bytes::Bytes;
use roboat::ClientBuilder;
use roboat::RoboatError;
use roboat::catalog::AssetType;
use roboat::ide::ide_types::NewStudioAsset;
use std::collections::HashMap;
use std::sync::Arc;

use super::ratelimiter::RateLimiter;
use super::tasks::{UploadContext, collect_upload_results};
const DEFAULT_CONCURRENT_TASKS: u64 = 50;

pub struct AssetUploader {
    pub roblosecurity: String,
    pub(super) rate_limiter: Arc<RateLimiter>,
}

impl AssetUploader {
    /// Creates a new AnimationUploader with a roblosecurity cookie.
    pub fn new(roblosecurity: String) -> Self {
        Self {
            roblosecurity,
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

        let asset = NewStudioAsset {
            asset_type,
            group_id,
            name: "reuploaded_animation".to_string(),
            description: "This is a example".to_string(),
            asset_data,
        };

        client.upload_studio_asset(asset).await
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
