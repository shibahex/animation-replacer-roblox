use crate::StudioParser;
use rbx_dom_weak::types::Variant;
use regex::Regex;
use roboat::assetdelivery::AssetBatchResponse;
use std::collections::HashSet;
use tracing::{debug, info};
use ustr::Ustr;

impl StudioParser {
    /// Returns a vector of AssetBatchResponse (Asset Details from batch API) found in the scripts
    /// # Notes:
    /// Takes in a script, scans all the IDs into it then has batch_sizes of 250.
    /// It posts 250 Ids at a time to the asset batch API, which returns the asset type of
    /// every id that is a valid asset.
    /// * Requires a cookie
    /// * Batch API does hang sometimes, fixed that with retries and 3 second timeout.
    pub async fn all_assets_in_scripts(
        &mut self,
        unformatted_search: bool,
    ) -> anyhow::Result<Vec<AssetBatchResponse>> {
        let id_list = self.asset_ids_in_scripts(unformatted_search);
        info!("Got asset ids from scripts: {}", id_list.len());
        self.fetch_animation_assets(id_list).await
    }

    /// Returns every asset id found in the source of all scripts, sorted and without duplicates.
    ///
    /// Without `unformatted_search` only ids written as `rbxassetid://ID` or
    /// `roblox.com/asset/?id=ID` are found. With it, every number with 6+ digits is found.
    pub fn asset_ids_in_scripts(&self, unformatted_search: bool) -> Vec<u64> {
        // This regex expression is FIND: "rbxassetid://" OR "roblox.com/asset?id=" THEN DIGITS
        let pattern = if unformatted_search {
            // Any large number (6+ digits)
            Regex::new(r"(\d{6,})").unwrap()
        } else {
            // Asset ID formats + fallback
            Regex::new(r"(?:rbxassetid:\/\/|roblox\.com\/asset\/?\?id=)(\d{6,})").unwrap()
        };

        // Collect and deduplicate all IDs from all scripts
        let mut all_ids: HashSet<u64> = HashSet::new();
        for script_ref in self.get_script_refs() {
            if let Some(instance) = self.dom.get_by_ref(script_ref)
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

        let mut id_list: Vec<u64> = all_ids.into_iter().collect();
        id_list.sort();
        debug!("Asset ids in scripts: {:?}", id_list);
        id_list
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
