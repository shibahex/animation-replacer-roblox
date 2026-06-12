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
        println!("{:?} {:?}", self.api_key, self.roblosecurity);
        match (&self.roblosecurity, &self.api_key) {
            (Some(cookie), Some(api_key)) => {
                Ok(AssetUploader::new(cookie.clone(), api_key.clone()))
            }
            (None, _) => Err(RoboatError::InvalidRoblosecurity),
            (_, None) => Err(RoboatError::InvalidRoblosecurity),
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
