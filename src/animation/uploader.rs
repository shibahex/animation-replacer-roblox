use bytes::Bytes;
use roboat::ClientBuilder;
use roboat::RoboatError;
use roboat::ide::ide_types::NewAnimation;
use std::collections::HashMap;
use std::sync::Arc;

use super::tasks::{RateLimiter, collect_upload_results, spawn_upload_tasks};

const DEFAULT_CONCURRENT_TASKS: u64 = 50;

pub struct AnimationUploader {
    pub roblosecurity: String,
    pub(super) rate_limiter: Arc<RateLimiter>,
}

impl AnimationUploader {
    /// Creates a new AnimationUploader with a roblosecurity cookie.
    pub fn new(roblosecurity: String) -> Self {
        Self {
            roblosecurity,
            rate_limiter: Arc::new(RateLimiter::new()),
        }
    }

    /// Uploads a single animation to Roblox.
    pub async fn upload_animation(
        &self,
        animation_data: Bytes,
        group_id: Option<u64>,
    ) -> Result<String, RoboatError> {
        let client = ClientBuilder::new()
            .roblosecurity(self.roblosecurity.clone())
            .build();

        let animation = NewAnimation {
            group_id,
            name: "reuploaded_animation".to_string(),
            description: "This is a example".to_string(),
            animation_data,
        };

        client.upload_new_animation(animation).await
    }

    /// Reuploads multiple animations concurrently.
    pub async fn reupload_all_animations(
        self: Arc<Self>,
        animations: Vec<roboat::assetdelivery::AssetBatchResponse>,
        group_id: Option<u64>,
        task_count: Option<u64>,
    ) -> Result<HashMap<String, String>, RoboatError> {
        let max_concurrent_tasks = task_count.unwrap_or(DEFAULT_CONCURRENT_TASKS);
        let total_animations = animations.len();

        let tasks = spawn_upload_tasks(
            self.clone(),
            animations,
            group_id,
            max_concurrent_tasks,
            total_animations,
        );

        collect_upload_results(tasks).await
    }
}
