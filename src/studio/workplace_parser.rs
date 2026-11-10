use super::file_format::read_dom;
use crate::StudioParser;
use rbx_types::{ContentType, Variant};
use regex::Regex;
use roboat::assetdelivery::AssetBatchResponse;
use std::collections::{HashMap, HashSet};
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

/// Roblox allows up to 50 characters in an asset name.
const MAX_ASSET_NAME_LENGTH: usize = 50;

/// Names Roblox gives by default that don't describe the asset. An accessory's part is always
/// called "Handle" and a SpecialMesh is called "Mesh", so the parent's name says more.
const DEFAULT_NAMES: &[&str] = &["Handle", "Mesh", "Part", "MeshPart"];

/// Returns true for names that don't say anything, like an Animation named "Animation".
fn is_default_name(name: &str, class: &str) -> bool {
    let name = name.trim();
    name.is_empty()
        || name.eq_ignore_ascii_case(class)
        || DEFAULT_NAMES
            .iter()
            .any(|default| name.eq_ignore_ascii_case(default))
}

/// Makes a name safe to upload: no control characters, and short enough for Roblox.
fn clean_asset_name(name: &str) -> String {
    name.chars()
        .filter(|c| !c.is_control())
        .take(MAX_ASSET_NAME_LENGTH)
        .collect::<String>()
        .trim()
        .to_string()
}

impl StudioParser {
    /// Picks a readable name for every asset id used by an instance, based on where it's used.
    ///
    /// In order of preference:
    /// 1. The instance's own name, or the attribute's name (`Run` for an Animation named Run)
    /// 2. The parent's name, when the instance has a default name (`Punch` for `Punch/Animation`)
    /// 3. The instance and property name (`Sky SkyboxBk`)
    ///
    /// Assets that are only used in scripts don't get a name.
    pub fn asset_names(&self) -> HashMap<u64, String> {
        // asset id -> (preference, name), lower preference wins
        let mut names: HashMap<u64, (u8, String)> = HashMap::new();
        let mut add = |id: u64, preference: u8, name: String| {
            let name = clean_asset_name(&name);
            if name.is_empty() {
                return;
            }
            let entry = names.entry(id).or_insert((u8::MAX, String::new()));
            if preference < entry.0 {
                *entry = (preference, name);
            }
        };

        for instance in self.dom.descendants() {
            for (property, value) in &instance.properties {
                if let Variant::Attributes(attributes) = value {
                    for (key, attribute) in attributes.iter() {
                        for id in asset_ids_in_property(key, attribute) {
                            add(id, 0, key.clone());
                        }
                    }
                    continue;
                }

                for id in asset_ids_in_property(property.as_str(), value) {
                    let (preference, name) = self.readable_name(instance, property.as_str());
                    add(id, preference, name);
                }
            }
        }

        names
            .into_iter()
            .map(|(id, (_, name))| (id, name))
            .collect()
    }

    /// A readable name for an asset used by `instance`'s `property`, with its preference.
    fn readable_name(&self, instance: &rbx_dom_weak::Instance, property: &str) -> (u8, String) {
        if !is_default_name(&instance.name, &instance.class) {
            return (0, instance.name.clone());
        }

        // Services (direct children of the root) aren't useful names, like "Lighting"
        if let Some(parent) = self.dom.get_by_ref(instance.parent())
            && parent.parent() != self.dom.root_ref()
            && parent.referent() != self.dom.root_ref()
            && !is_default_name(&parent.name, &parent.class)
        {
            return (1, parent.name.clone());
        }

        (2, format!("{} {}", instance.name, property))
    }

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
    fn picks_readable_asset_names() {
        use crate::StudioParser;
        use rbx_dom_weak::{InstanceBuilder, WeakDom};
        use rbx_types::{Attributes, Variant};

        let anim = |name: &str, id: &str| {
            InstanceBuilder::new("Animation")
                .with_name(name)
                .with_property("AnimationId", Variant::ContentId(id.into()))
        };
        let dom = WeakDom::new(
            InstanceBuilder::new("DataModel").with_child(
                InstanceBuilder::new("Workspace")
                    .with_child(anim("Run", "rbxassetid://111111"))
                    .with_child(
                        InstanceBuilder::new("Folder")
                            .with_name("Punch")
                            .with_child(anim("Animation", "rbxassetid://222222")),
                    )
                    .with_child(InstanceBuilder::new("Folder").with_property(
                        "Attributes",
                        Variant::Attributes(Attributes::new().with("WalkAnim", "333333")),
                    ))
                    .with_child(
                        InstanceBuilder::new("Accessory")
                            .with_name("CoolHat")
                            .with_child(
                                InstanceBuilder::new("MeshPart")
                                    .with_name("Handle")
                                    .with_property(
                                        "TextureID",
                                        Variant::ContentId("rbxassetid://555555".into()),
                                    ),
                            ),
                    )
                    .with_child(InstanceBuilder::new("Sky").with_property(
                        "SkyboxBk",
                        Variant::ContentId("rbxassetid://444444".into()),
                    )),
            ),
        );
        let parser = StudioParser {
            roblosecurity: None,
            api_key: None,
            dom,
        };

        let names = parser.asset_names();
        assert_eq!(names[&111111], "Run");
        assert_eq!(names[&222222], "Punch");
        assert_eq!(names[&333333], "WalkAnim");
        assert_eq!(names[&444444], "Sky SkyboxBk");
        assert_eq!(names[&555555], "CoolHat");
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
