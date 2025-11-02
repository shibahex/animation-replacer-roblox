use std::collections::HashMap;

use crate::AnimationUploader;
use roboat::ClientBuilder;

impl AnimationUploader {
    /// Fetches a place_id for an item owner
    pub async fn place_id(
        &self,
        asset_id: u64,
        cached_places: &mut HashMap<u64, Vec<u64>>, // place_id -> asset_ids
    ) -> anyhow::Result<u64> {
        let client = ClientBuilder::new()
            .roblosecurity(self.roblosecurity.to_string())
            .build();

        let asset_info = client.get_asset_info(asset_id).await?;

        if let Some(user_id) = asset_info.creation_context.creator.user_id {
            let user_id_parsed = user_id
                .parse::<u64>()
                .map_err(|e| anyhow::anyhow!("Failed to parse user_id '{}': {}", user_id, e))?;

            let place_id = self.user_places(user_id_parsed).await?;

            // record mapping place_id -> asset_id
            cached_places.entry(place_id).or_default().push(asset_id);

            return Ok(place_id);
        }

        if let Some(group_id) = asset_info.creation_context.creator.group_id {
            let group_id_parsed = group_id
                .parse::<u64>()
                .map_err(|e| anyhow::anyhow!("Failed to parse group_id '{}': {}", group_id, e))?;

            let place_id = self.group_places(group_id_parsed).await?;

            // record mapping place_id -> asset_id
            cached_places.entry(place_id).or_default().push(asset_id);

            return Ok(place_id);
        }

        Err(anyhow::anyhow!(
            "No user_id or group_id found for asset {}",
            asset_id
        ))
    }
}
mod internal {
    use crate::AnimationUploader;
    use bytes::Bytes;
    use reqwest::header::{HeaderMap, HeaderValue};
    use roboat::{
        ClientBuilder, RoboatError,
        assetdelivery::{AssetBatchPayload, AssetBatchResponse},
    };
    use tokio::time;

    impl AnimationUploader {
        /// Function for roboat getting root_place id for user place
        pub(super) async fn user_places(&self, user_id: u64) -> anyhow::Result<u64> {
            let client = ClientBuilder::new().build();
            let games_response = client.user_games(user_id).await?;
            if let Some(first_place) = games_response.data.first() {
                Ok(first_place.root_place.id)
            } else {
                Err(anyhow::anyhow!("Couldn't find place for user {}", user_id))
            }
        }

        /// Function for roboat getting root_place id for group place
        pub(super) async fn group_places(&self, group_id: u64) -> anyhow::Result<u64> {
            let client = ClientBuilder::new().build();
            let games_response = client.group_games(group_id).await?;
            if let Some(first_place) = games_response.data.first() {
                Ok(first_place.root_place.id)
            } else {
                Err(anyhow::anyhow!(
                    "Couldn't find place for group {}",
                    group_id
                ))
            }
        }

        /// Checks asset metadata for up to 250 assets.
        /// Now it also returns a place made by the creator, for place_id header to upload
        /// animations
        pub async fn check_asset_metadata(
            &self,
            asset_ids: Vec<AssetBatchPayload>,
            place_id: u64,
            timeout_secs: time::Duration,
        ) -> anyhow::Result<Option<Vec<AssetBatchResponse>>> {
            let mut headers = HeaderMap::new();
            headers.insert(
                "Roblox-Place-Id",
                HeaderValue::from_str(&place_id.to_string())?,
            );

            let timeout_client = reqwest::ClientBuilder::new()
                .timeout(timeout_secs)
                .default_headers(headers)
                .build()
                .map_err(|e| RoboatError::ReqwestError(e))?;

            let client = ClientBuilder::new()
                .roblosecurity(self.roblosecurity.clone())
                .reqwest_client(timeout_client)
                .build();

            match client.post_asset_metadata_batch(asset_ids).await {
                Ok(x) => Ok(Some(x)),
                Err(e) => Err(e.into()),
            }
        }

        /// Downloads file bytes from a URL with retry logic.
        ///
        /// # Examples
        ///
        /// ```rust
        /// let bytes = uploader.file_bytes_from_url("https://example.com/file.rbxm".to_string()).await?;
        /// ```
        pub async fn file_bytes_from_url(&self, url: String) -> Result<Bytes, RoboatError> {
            use reqwest::Client;
            use tokio::time::{Duration, timeout};

            let max_retries: usize = 3;
            const TIMEOUT_SECS: u64 = 15;

            let client = Client::new();

            for attempt in 1..=max_retries {
                let result =
                    timeout(Duration::from_secs(TIMEOUT_SECS), client.get(&url).send()).await;

                match result {
                    Ok(Ok(response)) => {
                        let bytes = response.bytes().await.map_err(RoboatError::ReqwestError)?;
                        return Ok(bytes);
                    }
                    Ok(Err(e)) => {
                        if attempt == max_retries {
                            return Err(RoboatError::ReqwestError(e))?;
                        }
                    }
                    Err(e) => {
                        println!("Getting file from animation url error: {:?}", e);
                        if attempt == max_retries {
                            return Err(RoboatError::InternalServerError);
                        }
                    }
                }
            }
            unreachable!("Loop should always return")
        }
    }
}
