use super::file_format::{is_xml_path, write_dom};
use super::workplace_parser::{asset_id_from_content, asset_id_from_string, content_url};
use crate::StudioParser;
use rbx_types::{Content, Variant};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

impl StudioParser {
    /// Goes through every property in the file that can hold an asset id (AnimationId,
    /// SoundId, Texture, StringValues, attributes, ...) and replaces the id if a new one is provided.
    pub fn update_game_assets(&mut self, asset_mapping: &HashMap<String, String>) {
        let instance_refs: Vec<_> = self
            .dom
            .descendants()
            .map(|instance| instance.referent())
            .collect();

        for instance_ref in instance_refs {
            let Some(instance) = self.dom.get_by_ref_mut(instance_ref) else {
                continue;
            };

            for (name, value) in instance.properties.iter_mut() {
                // Script sources are handled by update_script_assets
                if name.as_str() == "Source" {
                    continue;
                }
                replace_asset_id(value, asset_mapping);
            }
        }
    }

    /// Saves the DOM to a place file. Paths ending in `.rbxlx` or `.rbxmx` are saved as xml,
    /// anything else as binary.
    ///
    /// The file is written next to the target first and then moved over it, so a failed
    /// save never leaves a half written (corrupted) place file behind.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use animation_replace_roblox::StudioParser;
    /// # fn main() -> anyhow::Result<()> {
    /// let parser = StudioParser::builder()
    ///     .file_path("input.rbxl")
    ///     .build()?;
    /// parser.save_to_rbxl("output.rbxl")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn save_to_rbxl<P: AsRef<Path>>(&self, file_path: P) -> Result<(), anyhow::Error> {
        let path = file_path.as_ref().to_string_lossy();
        let expanded_path = PathBuf::from(shellexpand::full(&path)?.as_ref());

        let mut temp_name = expanded_path.as_os_str().to_owned();
        temp_name.push(".tmp");
        let temp_path = PathBuf::from(temp_name);

        let xml = is_xml_path(&expanded_path);

        let result = (|| -> anyhow::Result<()> {
            let mut writer = BufWriter::new(File::create(&temp_path)?);
            write_dom(&mut writer, &self.dom, xml)?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
            fs::rename(&temp_path, &expanded_path)?;
            Ok(())
        })();

        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        result
    }
}

/// Replaces the asset id in one property value, if the mapping has a new id for it.
///
/// Content properties become `rbxassetid://NEW`. Strings keep their format, so a
/// `StringValue` holding just `123` gets just the new number.
fn replace_asset_id(value: &mut Variant, asset_mapping: &HashMap<String, String>) {
    match value {
        Variant::String(text) => {
            if let Some(new_text) = replace_in_string(text, asset_mapping) {
                *text = new_text;
            }
        }
        Variant::Attributes(attributes) => {
            let replaced: Vec<(String, Variant)> = attributes
                .iter()
                .filter_map(|(key, attribute)| {
                    let mut attribute = attribute.clone();
                    let before = attribute.clone();
                    replace_asset_id(&mut attribute, asset_mapping);
                    (attribute != before).then(|| (key.clone(), attribute))
                })
                .collect();

            for (key, attribute) in replaced {
                attributes.insert(key, attribute);
            }
        }
        _ => {
            let Some(new_id) = content_url(value)
                .and_then(asset_id_from_content)
                .and_then(|old_id| asset_mapping.get(&old_id.to_string()))
            else {
                return;
            };

            let rbxasset = format!("rbxassetid://{}", new_id);
            *value = match value {
                Variant::Content(_) => Variant::Content(Content::from_uri(rbxasset)),
                _ => Variant::ContentId(rbxasset.into()),
            };
        }
    }
}

/// Returns the string with its asset id replaced, or `None` if it has no mapped asset id.
fn replace_in_string(text: &str, asset_mapping: &HashMap<String, String>) -> Option<String> {
    let old_id = asset_id_from_string(text)?.to_string();
    let new_id = asset_mapping.get(&old_id)?;
    Some(text.replace(&old_id, new_id))
}

#[cfg(test)]
mod tests {
    use super::replace_asset_id;
    use rbx_types::{Attributes, Variant};
    use std::collections::HashMap;

    #[test]
    fn replaces_ids_in_strings_and_attributes() {
        let mapping = HashMap::from([("507771019".to_string(), "999999999".to_string())]);

        let mut value = Variant::String("507771019".into());
        replace_asset_id(&mut value, &mapping);
        assert_eq!(value, Variant::String("999999999".into()));

        let mut value = Variant::String("rbxassetid://507771019".into());
        replace_asset_id(&mut value, &mapping);
        assert_eq!(value, Variant::String("rbxassetid://999999999".into()));

        let mut value =
            Variant::Attributes(Attributes::new().with("Anim", "rbxassetid://507771019"));
        replace_asset_id(&mut value, &mapping);
        let Variant::Attributes(attributes) = value else {
            panic!("not attributes")
        };
        assert_eq!(
            attributes.get("Anim"),
            Some(&Variant::String("rbxassetid://999999999".into()))
        );
    }
}
