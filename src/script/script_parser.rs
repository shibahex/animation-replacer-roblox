use crate::StudioParser;
use rbx_dom_weak::types::Variant;
use regex::Regex;
use roboat::assetdelivery::AssetBatchResponse;
use std::collections::HashSet;
use ustr::Ustr;

impl StudioParser {
    /// Returns a vector of AssetBatchResponse (Animation Details from batch API) found in the script
    /// # Notes:
    /// Takes in a script, scans all the IDs into it then has batch_sizes of 250.
    /// It posts 250 Ids at a time to the asset batch API then filters out everything but
    /// animations.
    /// * Requires a cookie
    /// * Batch API does hang sometimes, fixed that with retries and 3 second timeout.
    pub async fn all_animations_in_scripts(&mut self) -> anyhow::Result<Vec<AssetBatchResponse>> {
        let script_refs = self.get_script_refs();
        let pattern = Regex::new(r"rbxassetid:\/\/(\d{5,})").unwrap();

        // Collect and deduplicate all IDs from all scripts
        let mut all_ids: HashSet<u64> = HashSet::new();
        for script_ref in &script_refs {
            if let Some(instance) = self.dom.get_by_ref(*script_ref) {
                if let Some(Variant::String(source)) =
                    instance.properties.get(&Ustr::from("Source"))
                {
                    let ids_in_script = pattern
                        .find_iter(source)
                        .filter_map(|m| m.as_str().parse::<u64>().ok());

                    all_ids.extend(ids_in_script);
                }
            }
        }

        // Convert to Vec and fetch assets
        let mut id_list: Vec<u64> = all_ids.into_iter().collect();
        id_list.sort();
        println!("Got all animations from scripts: {}", id_list.len());
        self.fetch_animation_assets(id_list).await
    }

    /// Gets references to all script instances in the DOM.
    pub fn get_script_refs(&self) -> Vec<rbx_dom_weak::types::Ref> {
        self.dom
            .descendants()
            .filter(|instance| {
                matches!(
                    instance.class.as_str(),
                    "Script" | "LocalScript" | "ModuleScript"
                )
            })
            .map(|instance| instance.referent())
            .collect()
    }
}

mod internal {}
