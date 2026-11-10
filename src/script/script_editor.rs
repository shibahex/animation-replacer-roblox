use crate::StudioParser;
use rbx_types::Variant;
use regex::{Captures, Regex};
use std::collections::HashMap;
use ustr::Ustr;

impl StudioParser {
    /// Updates asset IDs in script source code using the provided mapping.
    ///
    /// Only whole numbers are replaced, so replacing `12345` won't touch `9123456`.
    pub fn update_script_assets(&mut self, asset_mapping: &HashMap<String, String>) {
        let number_pattern = Regex::new(r"\d+").unwrap();

        // Collect script refs first to avoid borrow checker issues
        let script_refs = self.get_script_refs();

        // Now modify each script
        for script_ref in script_refs {
            if let Some(instance) = self.dom.get_by_ref_mut(script_ref)
                && let Some(Variant::String(source)) =
                    instance.properties.get(&Ustr::from("Source"))
            {
                let new_source = number_pattern
                    .replace_all(source, |cap: &Captures| {
                        asset_mapping
                            .get(&cap[0])
                            .cloned()
                            .unwrap_or_else(|| cap[0].to_string())
                    })
                    .into_owned();

                // Update the source property
                instance
                    .properties
                    .insert(Ustr::from("Source"), Variant::String(new_source));
            }
        }
    }
}
