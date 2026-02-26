use crate::AssetUploader;
use crate::StudioParser;
use roboat::RoboatError;
use roboat::assetdelivery::AssetBatchResponse;

pub type UploadTask =
    tokio::task::JoinHandle<Result<(Option<String>, String), (Option<String>, RoboatError)>>;
pub mod info;
pub mod ratelimiter;
pub mod tasks;
pub mod uploader;
// Implement uploader code into the studio struct
impl StudioParser {
    pub fn animation_uploader(&self) -> Result<AssetUploader, RoboatError> {
        match &self.roblosecurity {
            Some(cookie) => Ok(AssetUploader::new(cookie.clone())),
            None => Err(RoboatError::InvalidRoblosecurity),
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
