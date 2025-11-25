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

        // This regex expression is FIND: "rbxassetid://" OR "roblox.com/asset?id=" THEN DIGITS
        let pattern = Regex::new(r"(?:rbxassetid:\/\/|roblox\.com\/asset\/\?id=)(\d{6,})").unwrap();

        // Collect and deduplicate all IDs from all scripts
        let mut all_ids: HashSet<u64> = HashSet::new();
        for script_ref in &script_refs {
            if let Some(instance) = self.dom.get_by_ref(*script_ref)
                && let Some(Variant::String(source)) =
                    instance.properties.get(&Ustr::from("Source"))
            {
                let cleaned_text: String = source
                    .trim()
                    .to_string()
                    .chars()
                    .filter(|c| !c.is_control())
                    .collect();
                // Iterate over all matches in the source
                for cap in pattern.captures_iter(&cleaned_text) {
                    if let Some(id_match) = cap.get(1)
                        && let Ok(id) = id_match.as_str().parse::<u64>()
                    {
                        all_ids.insert(id);
                    }
                }
            }
        }
        // Convert to Vec and fetch assets
        let mut id_list: Vec<u64> = all_ids.into_iter().collect();
        id_list.sort();
        println!("{:?}", id_list);
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
