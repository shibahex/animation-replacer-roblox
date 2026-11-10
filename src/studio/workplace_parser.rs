use crate::StudioParser;
use rbx_binary::from_reader;
use rbx_types::{ContentType, Variant};
use regex::Regex;
use roboat::assetdelivery::AssetBatchResponse;
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use std::sync::LazyLock;

/// Matches an asset id in a content url, like `rbxassetid://123` or
/// `http://www.roblox.com/asset/?id=123`, or a content url that is only digits.
static ASSET_ID_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)^\s*(?:rbxassetid://|(?:https?://)?(?:www\.)?roblox\.com/asset/?\?id=)?(\d+)\s*$",
    )
    .unwrap()
});

/// Gets the asset id out of a content url. Returns `None` for built-in content
/// like `rbxasset://textures/face.png`.
pub fn asset_id_from_content(content: &str) -> Option<u64> {
    ASSET_ID_PATTERN
        .captures(content)
        .and_then(|cap| cap.get(1))
        .and_then(|id| id.as_str().parse::<u64>().ok())
}

/// Gets the content url string out of a property, if it holds one.
/// Covers both the legacy `ContentId` type and the newer `Content` type.
pub fn content_url(value: &Variant) -> Option<&str> {
    match value {
        Variant::ContentId(content_id) => Some(content_id.as_str()),
        Variant::Content(content) => match content.value() {
            ContentType::Uri(uri) => Some(uri.as_str()),
            _ => None,
        },
        _ => None,
    }
}

impl StudioParser {
    /// Finds every asset referenced by an instance property in the file (AnimationId,
    /// SoundId, Texture, Image, MeshId, ...) and returns their metadata.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let parser = StudioParser::builder()
    ///     .file_path("MyPlace.rbxl")
    ///     .roblosecurity("cookie")
    ///     .build()?;
    /// let assets = parser.workspace_assets().await?;
    /// ```
    pub async fn workspace_assets(&self) -> anyhow::Result<Vec<AssetBatchResponse>> {
        let asset_ids = self.asset_ids_in_instances();
        self.fetch_animation_assets(asset_ids).await
    }

    /// Returns every asset id referenced by an instance property in the file,
    /// sorted and without duplicates.
    pub fn asset_ids_in_instances(&self) -> Vec<u64> {
        let asset_ids: HashSet<u64> = self
            .dom
            .descendants()
            .flat_map(|instance| instance.properties.values())
            .filter_map(content_url)
            .filter_map(asset_id_from_content)
            .collect();

        let mut asset_ids: Vec<u64> = asset_ids.into_iter().collect();
        asset_ids.sort();
        asset_ids
    }

    /// Creates a builder for fluent configuration with file path and authentication.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let parser = StudioParser::builder()
    ///     .file_path("~/Desktop/MyPlace.rbxl")
    ///     .roblosecurity("your_cookie")
    ///     .build()?;
    /// ```
    pub fn builder() -> StudioParserBuilder {
        StudioParserBuilder::new()
    }
}

/// Builder for creating StudioParser instances with optional authentication.
#[derive(Debug, Default)]
pub struct StudioParserBuilder {
    file_path: Option<String>,
    roblosecurity: Option<String>,
    api_key: Option<String>,
}

impl StudioParserBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn file_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.file_path = Some(path.as_ref().to_string_lossy().to_string());
        self
    }

    /// Sets the Roblosecurity cookie for API authentication.
    /// Required for animation validation and re-uploading features.
    pub fn roblosecurity<S: Into<String>>(mut self, roblosecurity: S) -> Self {
        self.roblosecurity = Some(roblosecurity.into());
        self
    }

    pub fn api_key<S: Into<String>>(mut self, api_key: S) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Builds the StudioParser. File path is required.
    pub fn build(self) -> Result<StudioParser, anyhow::Error> {
        let file_path = self
            .file_path
            .ok_or_else(|| anyhow::anyhow!("File path is required"))?;

        let expanded_path = shellexpand::full(&file_path)
            .map_err(|e| anyhow::anyhow!("Failed to expand path '{}': {}", file_path, e))?;

        let file = File::open(expanded_path.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to open file '{}': {}", expanded_path, e))?;

        let dom =
            from_reader(file).map_err(|e| anyhow::anyhow!("Failed to parse .rbxl DOM: {}", e))?;

        Ok(StudioParser {
            roblosecurity: self.roblosecurity,
            api_key: self.api_key,
            dom,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::asset_id_from_content;

    #[test]
    fn parses_asset_ids_from_content_urls() {
        assert_eq!(asset_id_from_content("rbxassetid://12345"), Some(12345));
        assert_eq!(
            asset_id_from_content("http://www.roblox.com/asset/?id=12345"),
            Some(12345)
        );
        assert_eq!(
            asset_id_from_content("https://www.roblox.com/asset?id=12345"),
            Some(12345)
        );
        assert_eq!(asset_id_from_content("12345"), Some(12345));
        assert_eq!(asset_id_from_content("rbxasset://textures/face.png"), None);
        assert_eq!(asset_id_from_content(""), None);
    }
}
