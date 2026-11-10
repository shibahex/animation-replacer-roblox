use bytes::Bytes;
use rbx_types::Variant;
use regex::Regex;
use roboat::Client;
use roboat::ClientBuilder;
use roboat::RoboatError;
use roboat::bedev2::AssetCreator;
use roboat::catalog::AssetType;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use super::ratelimiter::RateLimiter;
use super::tasks::{UploadContext, collect_upload_results};
use crate::studio::workplace_parser::{asset_id_from_content, content_url};
const DEFAULT_CONCURRENT_TASKS: u64 = 50;

/// How many times to try reading a freshly uploaded decal to find its image id.
/// New decals can take a few seconds before assetdelivery serves them.
const DECAL_IMAGE_RETRIES: u32 = 10;

pub struct AssetUploader {
    pub roblosecurity: String,
    pub api_key: String,
    pub(super) rate_limiter: Arc<RateLimiter>,
}

/// Returns true if the asset type can be reuploaded through Open Cloud.
///
/// Meshes, packages, accessories etc. can't be uploaded as-is, so they are skipped.
pub fn is_reuploadable(asset_type: AssetType) -> bool {
    matches!(
        asset_type,
        AssetType::Animation
            | AssetType::Audio
            | AssetType::Image
            | AssetType::Decal
            | AssetType::Model
            | AssetType::Video
    )
}

impl AssetUploader {
    /// Creates a new AssetUploader with a roblosecurity cookie and api key.
    pub fn new(roblosecurity: String, api_key: String) -> Self {
        Self {
            roblosecurity,
            api_key,
            rate_limiter: Arc::new(RateLimiter::new()),
        }
    }

    /// Uploads a single asset to Roblox and returns the new asset id.
    ///
    /// Images are uploaded as a Decal, then the image id inside that decal is returned,
    /// because properties like `ImageLabel.Image` need the image id, not the decal id.
    pub async fn upload_asset(
        &self,
        asset_data: Bytes,
        creator: AssetCreator,
        asset_type: AssetType,
    ) -> Result<String, RoboatError> {
        let client = ClientBuilder::new()
            .roblosecurity(self.roblosecurity.clone())
            .api_key(self.api_key.clone())
            .build();

        let upload_type = match asset_type {
            AssetType::Image => AssetType::Decal,
            other => other,
        };

        let resp = client
            .upload_asset(
                creator,
                format!("reuploaded_{}", asset_type),
                "Reuploaded with animation-replacer-roblox".to_string(),
                upload_type,
                &asset_data,
            )
            .await?;

        let new_asset_id = loop {
            let operation = client.poll_upload_operation(resp.path.clone()).await?;

            if operation.done {
                if let Some(asset_id) = operation.response.and_then(|resp| resp.asset_id) {
                    break asset_id;
                }

                return Err(match operation.error {
                    Some(error) => RoboatError::UnknownRobloxErrorCode {
                        code: error.code as u16,
                        message: error.message,
                    },
                    None => RoboatError::MalformedResponse,
                });
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        };

        info!("Uploaded complete! {:?} {}", asset_type, new_asset_id);

        if asset_type == AssetType::Image {
            let decal_id = new_asset_id
                .parse::<u64>()
                .map_err(|_| RoboatError::MalformedResponse)?;
            return image_id_from_decal(&client, decal_id)
                .await
                .map(|image_id| image_id.to_string());
        }

        Ok(new_asset_id)
    }

    /// Reuploads multiple assets concurrently.
    pub async fn reupload_all_assets(
        self: Arc<Self>,
        assets: Vec<roboat::assetdelivery::AssetBatchResponse>,
        group_id: Option<u64>,
        task_count: Option<u64>,
    ) -> Result<HashMap<String, String>, RoboatError> {
        let max_concurrent_tasks = task_count.unwrap_or(DEFAULT_CONCURRENT_TASKS);

        let (assets, skipped): (Vec<_>, Vec<_>) = assets
            .into_iter()
            .partition(|asset| asset.asset_type.is_some_and(is_reuploadable));

        for asset in &skipped {
            warn!(
                "Skipping {:?} {:?}: this asset type can't be reuploaded",
                asset.asset_type, asset.request_id
            );
        }

        let total_assets = assets.len();

        // Assets are uploaded under the group if one is given, otherwise under the cookie's user.
        let creator = match group_id {
            Some(group_id) => AssetCreator::Group(group_id),
            None => {
                let client = ClientBuilder::new()
                    .roblosecurity(self.roblosecurity.clone())
                    .build();
                AssetCreator::User(client.user_id().await?)
            }
        };

        let ctx = UploadContext::new(self, creator, max_concurrent_tasks, total_assets);

        let tasks = ctx.spawn_upload_tasks(assets);
        collect_upload_results(tasks).await
    }
}

/// Finds the image id inside a decal, by downloading the decal and reading its `Texture`.
///
/// Retries for a while, because a decal that was just uploaded isn't served right away.
async fn image_id_from_decal(client: &Client, decal_id: u64) -> Result<u64, RoboatError> {
    let mut last_error = RoboatError::MalformedResponse;

    for attempt in 1..=DECAL_IMAGE_RETRIES {
        match client.fetch_asset_data(decal_id).await {
            Ok(decal_data) => {
                if let Some(image_id) = texture_id_from_decal_data(&decal_data) {
                    debug!("Decal {} has image id {}", decal_id, image_id);
                    return Ok(image_id);
                }
                last_error = RoboatError::MalformedResponse;
            }
            Err(e) => last_error = e,
        }

        debug!(
            "Decal {} not ready yet (attempt {}/{}): {}",
            decal_id, attempt, DECAL_IMAGE_RETRIES, last_error
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    warn!("Couldn't find the image id for decal {}", decal_id);
    Err(last_error)
}

/// Reads the texture id out of a decal model, which can be binary (.rbxm) or xml (.rbxmx).
fn texture_id_from_decal_data(decal_data: &[u8]) -> Option<u64> {
    if decal_data.starts_with(b"<roblox!") {
        let dom = rbx_binary::from_reader(decal_data).ok()?;
        return dom
            .descendants()
            .filter(|instance| instance.class == "Decal")
            .filter_map(|instance| instance.properties.get(&ustr::ustr("Texture")))
            .filter_map(|value: &Variant| content_url(value))
            .find_map(asset_id_from_content);
    }

    let text = String::from_utf8_lossy(decal_data);
    let texture_pattern = Regex::new(r#"(?s)name="Texture"[^>]*>.*?<url>([^<]*)</url>"#).unwrap();
    texture_pattern
        .captures(&text)
        .and_then(|cap| asset_id_from_content(&cap[1]))
}

#[cfg(test)]
mod tests {
    use super::texture_id_from_decal_data;

    #[test]
    fn reads_texture_id_from_xml_decal() {
        let decal = br#"<roblox version="4"><Item class="Decal"><Properties>
            <Content name="Texture"><url>http://www.roblox.com/asset/?id=987654</url></Content>
            </Properties></Item></roblox>"#;
        assert_eq!(texture_id_from_decal_data(decal), Some(987654));
    }
}
