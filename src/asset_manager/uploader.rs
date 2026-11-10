use bytes::Bytes;
use rbx_types::Variant;
use regex::Regex;
use roboat::Client;
use roboat::ClientBuilder;
use roboat::RoboatError;
use roboat::bedev2::AssetCreator;
use roboat::catalog::AssetType;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use super::is_retryable;
use super::ratelimiter::RateLimiter;
use super::tasks::{UploadContext, collect_upload_results};
use crate::studio::file_format::{read_dom, write_dom};
use crate::studio::workplace_parser::{asset_id_from_content, content_url};
pub const DEFAULT_CONCURRENT_TASKS: u64 = 5;

/// How long to wait for Roblox to finish processing an upload.
const MAX_UPLOAD_WAIT: Duration = Duration::from_secs(300);

/// How many times to try reading a freshly uploaded decal to find its image id.
/// New decals can take a few seconds before assetdelivery serves them.
const DECAL_IMAGE_RETRIES: u32 = 10;

pub struct AssetUploader {
    pub roblosecurity: String,
    pub api_key: String,
    pub(super) rate_limiter: Arc<RateLimiter>,
    /// Shared roboat client with the cookie and api key, so connections are reused.
    pub(super) client: Arc<Client>,
    /// Plain http client for downloading files from the CDN.
    pub(super) http: reqwest::Client,
    /// Roboat clients that send a `Roblox-Place-Id` header, one per place.
    pub(super) place_clients: Mutex<HashMap<u64, Arc<Client>>>,
    /// Remembers which places can access private assets, shared by every metadata fetch.
    pub(super) place_cache: Mutex<PlaceCache>,
    /// Readable names for the reuploaded assets, by their old asset id.
    pub(super) asset_names: Mutex<HashMap<u64, String>>,
}

/// Remembers places that can access private assets, so the same lookups aren't repeated.
#[derive(Default)]
pub struct PlaceCache {
    /// Places that unlocked private assets before, the most recent first.
    pub working_places: Vec<u64>,
    /// A creator's public places, most visited first. Empty if they have none.
    pub creator_places: HashMap<AssetCreator, Vec<u64>>,
}

/// The reqwest client settings used for every request, so a stuck request can't hang forever.
pub(super) fn http_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(120))
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
        let http = http_client_builder()
            .build()
            .expect("Failed to build the http client");

        let client = ClientBuilder::new()
            .roblosecurity(roblosecurity.clone())
            .api_key(api_key.clone())
            .reqwest_client(http.clone())
            .build();

        Self {
            roblosecurity,
            api_key,
            rate_limiter: Arc::new(RateLimiter::new()),
            client: Arc::new(client),
            http,
            place_clients: Mutex::new(HashMap::new()),
            place_cache: Mutex::new(PlaceCache::default()),
            asset_names: Mutex::new(HashMap::new()),
        }
    }

    /// Adds readable names (by old asset id) to use for the reuploaded assets.
    /// Names that are already known are kept.
    pub fn add_asset_names(&self, names: HashMap<u64, String>) {
        let mut asset_names = self.asset_names.lock().unwrap();
        for (id, name) in names {
            asset_names.entry(id).or_insert(name);
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
        let operation_path = self
            .start_upload(&asset_data, creator, asset_type, None)
            .await?;
        self.finish_upload(operation_path, asset_type).await
    }

    /// Starts an upload and returns the operation path to wait on with [`Self::finish_upload`].
    ///
    /// `source_id` is the old asset id. It's used to name the new asset after the instance
    /// that uses it (see [`Self::add_asset_names`]), and is saved in the description.
    ///
    /// This is the only step that is safe to retry, since a failed start doesn't create an asset.
    pub async fn start_upload(
        &self,
        asset_data: &Bytes,
        creator: AssetCreator,
        asset_type: AssetType,
        source_id: Option<u64>,
    ) -> Result<String, RoboatError> {
        let upload_type = match asset_type {
            AssetType::Image => AssetType::Decal,
            other => other,
        };

        // Open Cloud rejects xml animations and models with Bad Request, so old .rbxmx assets
        // are converted to binary first
        let converted;
        let asset_data = if matches!(upload_type, AssetType::Animation | AssetType::Model)
            && asset_data.starts_with(b"<roblox ")
        {
            converted = xml_to_binary(asset_data)?;
            &converted
        } else {
            asset_data
        };

        let default_name = format!("reuploaded_{}", asset_type);
        let name = source_id
            .and_then(|id| self.asset_names.lock().unwrap().get(&id).cloned())
            .unwrap_or_else(|| default_name.clone());
        let description = match source_id {
            Some(id) => format!("Reuploaded from {} with animation-replacer-roblox", id),
            None => "Reuploaded with animation-replacer-roblox".to_string(),
        };

        let result = self
            .client
            .upload_asset(
                creator,
                name.clone(),
                description.clone(),
                upload_type,
                asset_data,
            )
            .await;

        // A name taken from the game can be rejected (for example by moderation),
        // so try once more with the default name
        let result = match result {
            Err(RoboatError::BadRequest | RoboatError::UnknownRobloxErrorCode { .. })
                if name != default_name =>
            {
                debug!(
                    "Upload named {:?} was rejected, retrying with the default name",
                    name
                );
                self.client
                    .upload_asset(creator, default_name, description, upload_type, asset_data)
                    .await
            }
            other => other,
        };

        Ok(result?.path)
    }

    /// Waits for an upload to finish and returns the new asset id. For images, returns
    /// the image id inside the uploaded decal.
    ///
    /// Network errors and rate limits while waiting are retried here, so the asset is never
    /// uploaded a second time just because checking on it failed.
    pub async fn finish_upload(
        &self,
        operation_path: String,
        asset_type: AssetType,
    ) -> Result<String, RoboatError> {
        let started = Instant::now();
        let mut poll_delay = Duration::from_secs(1);

        let new_asset_id = loop {
            if started.elapsed() > MAX_UPLOAD_WAIT {
                warn!(
                    "Gave up waiting for upload {} after {:?}",
                    operation_path, MAX_UPLOAD_WAIT
                );
                return Err(RoboatError::InternalServerError);
            }

            tokio::time::sleep(poll_delay).await;
            poll_delay = (poll_delay * 2).min(Duration::from_secs(5));
            self.rate_limiter.wait_if_limited().await;

            let operation = match self
                .client
                .poll_upload_operation(operation_path.clone())
                .await
            {
                Ok(operation) => operation,
                Err(e) if is_retryable(&e) => {
                    debug!("Checking upload {} failed, retrying: {}", operation_path, e);
                    continue;
                }
                Err(e) => return Err(e),
            };

            if !operation.done {
                continue;
            }

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
        };

        debug!("Upload finished: {:?} {}", asset_type, new_asset_id);

        if asset_type == AssetType::Image {
            let decal_id = new_asset_id
                .parse::<u64>()
                .map_err(|_| RoboatError::MalformedResponse)?;
            return image_id_from_decal(&self.client, decal_id)
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

        // Assets are uploaded under the group if one is given, otherwise under the cookie's user.
        let creator = match group_id {
            Some(group_id) => AssetCreator::Group(group_id),
            None => AssetCreator::User(self.client.user_id().await?),
        };

        // Models go last, so the assets inside them that are also used in the place are
        // already uploaded and don't get uploaded twice.
        let (models, assets): (Vec<_>, Vec<_>) = assets
            .into_iter()
            .partition(|asset| asset.asset_type == Some(AssetType::Model));

        let ctx = UploadContext::new(self, creator, max_concurrent_tasks, assets.len());

        let tasks = ctx.spawn_upload_tasks(assets);
        let mut asset_mapping = collect_upload_results(tasks).await?;

        if !models.is_empty() {
            ctx.reupload_models(models, &mut asset_mapping).await;
        }

        Ok(asset_mapping)
    }
}

/// Converts an xml model or animation (.rbxmx) to binary (.rbxm).
fn xml_to_binary(asset_data: &Bytes) -> Result<Bytes, RoboatError> {
    let dom = read_dom(asset_data)
        .map_err(|e| RoboatError::UnsupportedAsset(format!("unreadable xml file: {}", e)))?;

    let mut binary = Vec::new();
    write_dom(&mut binary, &dom, false)
        .map_err(|e| RoboatError::UnsupportedAsset(format!("couldn't convert xml file: {}", e)))?;

    debug!("Converted an xml asset to binary");
    Ok(Bytes::from(binary))
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

    static TEXTURE_PATTERN: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"(?s)name="Texture"[^>]*>.*?<url>([^<]*)</url>"#).unwrap());

    let text = String::from_utf8_lossy(decal_data);
    TEXTURE_PATTERN
        .captures(&text)
        .and_then(|cap| asset_id_from_content(&cap[1]))
}

#[cfg(test)]
mod tests {
    use super::texture_id_from_decal_data;

    #[test]
    fn converts_xml_animations_to_binary() {
        let xml = bytes::Bytes::from_static(
            br#"<roblox version="4"><Item class="KeyframeSequence" referent="RBX0"><Properties>
            <string name="Name">Anim</string></Properties></Item></roblox>"#,
        );
        let binary = super::xml_to_binary(&xml).unwrap();
        assert!(binary.starts_with(b"<roblox!"));
    }

    #[test]
    fn reads_texture_id_from_xml_decal() {
        let decal = br#"<roblox version="4"><Item class="Decal"><Properties>
            <Content name="Texture"><url>http://www.roblox.com/asset/?id=987654</url></Content>
            </Properties></Item></roblox>"#;
        assert_eq!(texture_id_from_decal_data(decal), Some(987654));
    }
}
