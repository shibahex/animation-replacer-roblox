use super::file_format::read_dom;
use crate::StudioParser;
use rbx_types::{ContentType, Variant};
use regex::Regex;
use roboat::assetdelivery::AssetBatchResponse;
use std::collections::HashSet;
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

/// Gets the asset id out of a string value (like a `StringValue` or a string attribute).
///
/// Strings are checked more strictly than content properties: a bare number needs at least
/// 6 digits to count as an asset id, so values like `"5"` aren't looked up.
pub fn asset_id_from_string(value: &str) -> Option<u64> {
    let id = asset_id_from_content(value)?;
    let is_bare_number = value.trim().bytes().all(|b| b.is_ascii_digit());

    if is_bare_number && id < 100_000 {
        return None;
    }
    Some(id)
}

/// Gets every asset id referenced by one instance property.
///
/// Checks content properties (`AnimationId`, `SoundId`, `Texture`, ...), string properties
/// (like `StringValue.Value`) and string attributes. Script sources are skipped here, they're
/// scanned separately.
pub fn asset_ids_in_property(name: &str, value: &Variant) -> Vec<u64> {
    match value {
        Variant::String(_) if name == "Source" => Vec::new(),
        Variant::String(text) => asset_id_from_string(text).into_iter().collect(),
        Variant::Attributes(attributes) => attributes
            .iter()
            .filter_map(|(_, attribute)| match attribute {
                Variant::String(text) => asset_id_from_string(text),
                other => content_url(other).and_then(asset_id_from_content),
            })
            .collect(),
        other => content_url(other)
            .and_then(asset_id_from_content)
            .into_iter()
            .collect(),
    }
}

impl StudioParser {
    /// Finds every asset referenced by an instance property in the file (AnimationId,
    /// SoundId, Texture, Image, MeshId, StringValues, attributes, ...) and returns their metadata.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use animation_replace_roblox::StudioParser;
    /// # async fn run() -> anyhow::Result<()> {
    /// let parser = StudioParser::builder()
    ///     .file_path("MyPlace.rbxl")
    ///     .roblosecurity("cookie")
    ///     .api_key("api_key")
    ///     .build()?;
    /// let assets = parser.workspace_assets().await?;
    /// # Ok(())
    /// # }
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
            .flat_map(|instance| instance.properties.iter())
            .flat_map(|(name, value)| asset_ids_in_property(name.as_str(), value))
            .collect();

        let mut asset_ids: Vec<u64> = asset_ids.into_iter().collect();
        asset_ids.sort();
        asset_ids
    }

    /// Creates a builder for fluent configuration with file path and authentication.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use animation_replace_roblox::StudioParser;
    /// # fn main() -> anyhow::Result<()> {
    /// let parser = StudioParser::builder()
    ///     .file_path("~/Desktop/MyPlace.rbxl")
    ///     .roblosecurity("your_cookie")
    ///     .build()?;
    /// # Ok(())
    /// # }
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

        let data = std::fs::read(expanded_path.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to open file '{}': {}", expanded_path, e))?;

        let dom = read_dom(&data)
            .map_err(|e| anyhow::anyhow!("Failed to parse '{}': {}", expanded_path, e))?;

        Ok(StudioParser {
            roblosecurity: self.roblosecurity,
            api_key: self.api_key,
            dom,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{asset_id_from_content, asset_id_from_string, asset_ids_in_property};

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

    #[test]
    fn parses_asset_ids_from_strings() {
        assert_eq!(asset_id_from_string("rbxassetid://123"), Some(123));
        assert_eq!(asset_id_from_string("507771019"), Some(507771019));
        assert_eq!(asset_id_from_string("5"), None);
        assert_eq!(asset_id_from_string("hello 507771019"), None);
    }

    #[test]
    fn finds_asset_ids_in_properties() {
        use rbx_types::{Attributes, Variant};

        let attributes = Attributes::new()
            .with("WalkAnim", "rbxassetid://507777826")
            .with("Speed", 16.0_f64);
        assert_eq!(
            asset_ids_in_property("Attributes", &Variant::Attributes(attributes)),
            vec![507777826]
        );
        assert_eq!(
            asset_ids_in_property("Value", &Variant::String("507771019".into())),
            vec![507771019]
        );
        assert!(asset_ids_in_property("Source", &Variant::String("507771019".into())).is_empty());
    }
}
