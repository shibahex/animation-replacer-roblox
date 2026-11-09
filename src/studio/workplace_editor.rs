use super::workplace_parser::{asset_id_from_content, content_url};
use crate::StudioParser;
use rbx_binary::to_writer;
use rbx_types::{Content, Variant};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

impl StudioParser {
    /// Goes through every content property in the file (AnimationId, SoundId, Texture, ...)
    /// and replaces the asset id if a new one is provided.
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

            for value in instance.properties.values_mut() {
                let Some(new_id) = content_url(value)
                    .and_then(asset_id_from_content)
                    .and_then(|old_id| asset_mapping.get(&old_id.to_string()))
                else {
                    continue;
                };

                let rbxasset = format!("rbxassetid://{}", new_id);
                *value = match value {
                    Variant::Content(_) => Variant::Content(Content::from_uri(rbxasset)),
                    _ => Variant::ContentId(rbxasset.into()),
                };
            }
        }
    }

    /// Saves the DOM to a .rbxl file.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let parser = StudioParser::builder()
    ///     .file_path("input.rbxl")
    ///     .build()?;
    /// parser.save_to_rbxl("output.rbxl")?;
    /// ```
    pub fn save_to_rbxl<P: AsRef<Path>>(&self, file_path: P) -> Result<(), anyhow::Error> {
        let expanded_path = shellexpand::full(file_path.as_ref().to_str().unwrap())?;
        let file = File::create(expanded_path.as_ref())?;

        // Get the children of the root instead of the root
        let root_children = self.dom.get_by_ref(self.dom.root_ref()).unwrap().children();

        to_writer(file, &self.dom, root_children)?;
        Ok(())
    }
}
