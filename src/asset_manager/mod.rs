use crate::AssetUploader;
use crate::StudioParser;
use roboat::RoboatError;
use roboat::assetdelivery::AssetBatchResponse;

pub type UploadTask =
    tokio::task::JoinHandle<Result<(Option<String>, String), (Option<String>, RoboatError)>>;
pub mod info;
pub mod mesh;
pub mod models;
pub mod ratelimiter;
pub mod tasks;
pub mod uploader;
// Implement uploader code into the studio struct
impl StudioParser {
    pub fn animation_uploader(&self) -> Result<AssetUploader, RoboatError> {
        match (&self.roblosecurity, &self.api_key) {
            (Some(cookie), Some(api_key)) => {
                Ok(AssetUploader::new(cookie.clone(), api_key.clone()))
            }
            (None, _) => Err(RoboatError::RoblosecurityNotSet),
            (_, None) => Err(RoboatError::ApiKeyNotSet),
        }
    }
    pub async fn fetch_animation_assets(
        &self,
        asset_ids: Vec<u64>,
    ) -> anyhow::Result<Vec<AssetBatchResponse>> {
        let uploader = self.animation_uploader()?;
        uploader.fetch_animation_assets(asset_ids).await
    }
}

/// Returns true for errors that can work on a retry: rate limits, timeouts,
/// network errors and server errors.
pub(crate) fn is_retryable(error: &RoboatError) -> bool {
    match error {
        RoboatError::TooManyRequests
        | RoboatError::InternalServerError
        | RoboatError::MalformedResponse
        | RoboatError::ReqwestError(_) => true,
        RoboatError::UnidentifiedStatusCode(code) => matches!(code, 502..=504),
        _ => false,
    }
}
