use crate::StudioParser;
use rbx_binary::from_reader;
use rbx_types::Variant;
use regex::Regex;
use roboat::assetdelivery::AssetBatchResponse;
use std::fs::File;
use std::path::Path;
use ustr::Ustr;

impl StudioParser {
    /// Finds Animation instances in the workspace and returns their metadata.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let parser = StudioParser::builder()
    ///     .file_path("MyPlace.rbxl")
    ///     .roblosecurity("cookie")
    ///     .build()?;
    /// let animations = parser.workspace_animations().await?;
    /// ```
    pub async fn workspace_animations(&self) -> anyhow::Result<Vec<AssetBatchResponse>> {
        let re = Regex::new(r"\d+").unwrap();

        let mut asset_ids: Vec<u64> = self
            .dom
            .descendants()
            .filter(|instance| instance.class == "Animation")
            .filter_map(
                |instance| match instance.properties.get(&Ustr::from("AnimationId")) {
                    Some(Variant::ContentId(content_id)) => re
                        .find(content_id.as_str())
                        .and_then(|mat| mat.as_str().parse::<u64>().ok()),
                    _ => None,
                },
            )
            .collect();

        asset_ids.sort();
        self.fetch_animation_assets(asset_ids).await
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
            dom,
        })
    }
}
