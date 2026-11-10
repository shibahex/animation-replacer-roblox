use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderValue};
use roboat::{
    Client, ClientBuilder, RoboatError,
    assetdelivery::{AssetBatchPayload, AssetBatchResponse},
    bedev2::AssetCreator,
};
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{debug, info, warn};

use super::is_retryable;
use crate::AssetUploader;

const BATCH_SIZE: usize = 250;
/// How many times an API call is tried before giving up.
const MAX_API_ATTEMPTS: u32 = 6;
/// How many of a creator's games are tried when looking for one that can access their assets.
const MAX_CANDIDATE_PLACES: usize = 10;
/// The batch API returns this when an asset is private and needs a `Roblox-Place-Id`
/// of a game that is allowed to use it.
const PRIVATE_ASSET_CODE: u16 = 403;

impl AssetUploader {
    /// Fetches metadata (asset type and download location) for multiple assets.
    ///
    /// Private assets need a `Roblox-Place-Id` header of a game that can use them. For those,
    /// places that already worked are tried first, then the games of the asset's creator
    /// (most visited first). Ids that aren't assets are dropped.
    pub async fn fetch_animation_assets(
        &self,
        asset_ids: Vec<u64>,
    ) -> anyhow::Result<Vec<AssetBatchResponse>> {
        let mut found = Vec::new();
        let mut private = Vec::new();

        for batch in asset_ids.chunks(BATCH_SIZE) {
            let responses = self
                .retry_api("asset batch", || self.batch_metadata(batch, None))
                .await?;
            split_batch_responses(responses, &mut found, &mut private);
        }

        if !private.is_empty() {
            info!(
                "{} assets are private, finding games that can access them",
                private.len()
            );
            self.resolve_private_assets(private, &mut found).await;
        }

        Ok(found)
    }

    /// Downloads file bytes from a URL with retry logic.
    pub async fn file_bytes_from_url(&self, url: String) -> Result<Bytes, RoboatError> {
        const MAX_RETRIES: u32 = 3;

        let mut last_error = RoboatError::InternalServerError;

        for attempt in 1..=MAX_RETRIES {
            let result = self
                .http
                .get(&url)
                .send()
                .await
                .and_then(|response| response.error_for_status());

            match result {
                Ok(response) => match response.bytes().await {
                    Ok(bytes) => return Ok(bytes),
                    Err(e) => last_error = RoboatError::ReqwestError(e),
                },
                Err(e) => {
                    // A 4xx (like an expired download link) won't work on a retry
                    let is_client_error = e.status().is_some_and(|s| s.is_client_error());
                    if is_client_error {
                        return Err(RoboatError::ReqwestError(e));
                    }
                    last_error = RoboatError::ReqwestError(e);
                }
            }

            warn!(
                "Download failed (attempt {}/{}): {}",
                attempt, MAX_RETRIES, last_error
            );
            tokio::time::sleep(Duration::from_secs(2 * attempt as u64)).await;
        }

        Err(last_error)
    }

    /// Calls an API, retrying on rate limits, timeouts and server errors.
    ///
    /// Rate limits pause every task through the shared rate limiter. Errors that won't
    /// change on a retry (like an invalid cookie) are returned right away.
    pub(super) async fn retry_api<T, F, Fut>(&self, what: &str, mut call: F) -> anyhow::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, RoboatError>>,
    {
        for attempt in 1..=MAX_API_ATTEMPTS {
            self.rate_limiter.wait_if_limited().await;

            match call().await {
                Ok(value) => return Ok(value),
                Err(RoboatError::TooManyRequests) if attempt < MAX_API_ATTEMPTS => {
                    let wait = (5 * 2_u64.pow(attempt - 1)).min(60);
                    warn!(
                        "{} got rate limited (attempt {}/{}), waiting {} seconds",
                        what, attempt, MAX_API_ATTEMPTS, wait
                    );
                    self.rate_limiter.set_rate_limit(wait).await;
                }
                Err(e) if is_retryable(&e) && attempt < MAX_API_ATTEMPTS => {
                    warn!(
                        "{} failed (attempt {}/{}), retrying: {}",
                        what, attempt, MAX_API_ATTEMPTS, e
                    );
                    tokio::time::sleep(Duration::from_secs(2 * attempt as u64)).await;
                }
                Err(e) => return Err(anyhow::anyhow!("{} failed: {}", what, e)),
            }
        }

        unreachable!("the last attempt always returns")
    }

    /// Posts up to 250 assets to the asset batch API, optionally with a `Roblox-Place-Id`.
    async fn batch_metadata(
        &self,
        asset_ids: &[u64],
        place_id: Option<u64>,
    ) -> Result<Vec<AssetBatchResponse>, RoboatError> {
        let client = match place_id {
            Some(place_id) => self.place_client(place_id)?,
            None => Arc::clone(&self.client),
        };

        client
            .post_asset_metadata_batch(create_batch_payloads(asset_ids))
            .await
    }

    /// Returns a client that sends the `Roblox-Place-Id` header, reusing it for the same place.
    fn place_client(&self, place_id: u64) -> Result<Arc<Client>, RoboatError> {
        let mut clients = self.place_clients.lock().unwrap();
        if let Some(client) = clients.get(&place_id) {
            return Ok(Arc::clone(client));
        }

        let mut headers = HeaderMap::new();
        headers.insert(
            "Roblox-Place-Id",
            HeaderValue::from_str(&place_id.to_string()).map_err(|_| RoboatError::BadRequest)?,
        );

        let reqwest_client = super::uploader::http_client_builder()
            .default_headers(headers)
            .build()
            .map_err(RoboatError::ReqwestError)?;

        let client = Arc::new(
            ClientBuilder::new()
                .roblosecurity(self.roblosecurity.clone())
                .reqwest_client(reqwest_client)
                .build(),
        );
        clients.insert(place_id, Arc::clone(&client));
        Ok(client)
    }

    /// Finds games that can access the private assets, and adds their metadata to `found`.
    async fn resolve_private_assets(
        &self,
        mut pending: Vec<u64>,
        found: &mut Vec<AssetBatchResponse>,
    ) {
        // Places that worked before usually work again, since assets in one game tend to
        // come from the same few creators. This costs one batch call per place.
        let known_places = self.place_cache.lock().unwrap().working_places.clone();
        for place_id in known_places {
            if pending.is_empty() {
                return;
            }
            self.try_place(place_id, &mut pending, found).await;
        }

        let mut checked_creators: HashSet<AssetCreator> = HashSet::new();
        let mut unresolved: Vec<u64> = Vec::new();

        while let Some(&asset_id) = pending.first() {
            let creator = match self.asset_creator(asset_id).await {
                Ok(creator) => creator,
                Err(e) => {
                    debug!("Couldn't get the creator of asset {}: {}", asset_id, e);
                    unresolved.push(pending.remove(0));
                    continue;
                }
            };

            // Every place of this creator was already tried with all pending assets
            if !checked_creators.insert(creator) {
                unresolved.push(pending.remove(0));
                continue;
            }

            for place_id in self.creator_places(creator).await {
                if !pending.contains(&asset_id) {
                    break;
                }
                self.try_place(place_id, &mut pending, found).await;
            }

            if let Some(index) = pending.iter().position(|&id| id == asset_id) {
                unresolved.push(pending.remove(index));
            }
        }

        if !unresolved.is_empty() {
            warn!(
                "{} private assets couldn't be accessed and won't be reuploaded \
                 (no game was found that is allowed to use them): {:?}",
                unresolved.len(),
                unresolved
            );
        }
    }

    /// Fetches the pending assets with a place id, moving the ones it unlocked into `found`.
    async fn try_place(
        &self,
        place_id: u64,
        pending: &mut Vec<u64>,
        found: &mut Vec<AssetBatchResponse>,
    ) {
        let mut unlocked = 0;

        for batch in pending.clone().chunks(BATCH_SIZE) {
            let responses = match self
                .retry_api("asset batch", || self.batch_metadata(batch, Some(place_id)))
                .await
            {
                Ok(responses) => responses,
                Err(e) => {
                    warn!("Couldn't check assets with place {}: {}", place_id, e);
                    return;
                }
            };

            for response in responses {
                if response.errors.is_some() {
                    continue;
                }
                if let Some(asset_id) = response
                    .request_id
                    .as_ref()
                    .and_then(|id| id.parse::<u64>().ok())
                {
                    pending.retain(|&id| id != asset_id);
                    found.push(response);
                    unlocked += 1;
                }
            }
        }

        if unlocked > 0 {
            info!("Place {} unlocked {} private assets", place_id, unlocked);
            let mut cache = self.place_cache.lock().unwrap();
            cache.working_places.retain(|&id| id != place_id);
            cache.working_places.insert(0, place_id);
        }
    }

    /// Gets who created an asset.
    async fn asset_creator(&self, asset_id: u64) -> anyhow::Result<AssetCreator> {
        let asset_info = self
            .retry_api("asset info", || self.client.get_asset_info(asset_id))
            .await?;
        let creator = asset_info.creation_context.creator;

        if let Some(user_id) = creator.user_id.and_then(|id| id.parse().ok()) {
            return Ok(AssetCreator::User(user_id));
        }
        if let Some(group_id) = creator.group_id.and_then(|id| id.parse().ok()) {
            return Ok(AssetCreator::Group(group_id));
        }

        Err(anyhow::anyhow!(
            "No user or group found for asset {}",
            asset_id
        ))
    }

    /// Gets a creator's public games, most visited first. Cached, so each creator is only
    /// looked up once. Returns an empty list if they have none or the lookup fails.
    async fn creator_places(&self, creator: AssetCreator) -> Vec<u64> {
        if let Some(places) = self
            .place_cache
            .lock()
            .unwrap()
            .creator_places
            .get(&creator)
        {
            return places.clone();
        }

        let games = match creator {
            AssetCreator::User(id) => {
                self.retry_api("user games", || {
                    self.client.user_games(id, Some(true), None)
                })
                .await
            }
            AssetCreator::Group(id) => {
                self.retry_api("group games", || {
                    self.client.group_games(id, Some(true), None)
                })
                .await
            }
        };

        let places = match games {
            Ok(mut games) => {
                games
                    .data
                    .sort_by_key(|game| std::cmp::Reverse(game.place_visits));
                games
                    .data
                    .iter()
                    .take(MAX_CANDIDATE_PLACES)
                    .map(|game| game.root_place.id)
                    .collect()
            }
            Err(e) => {
                debug!("Couldn't get the games of {:?}: {}", creator, e);
                Vec::new()
            }
        };

        debug!("Places for {:?}: {:?}", creator, places);
        self.place_cache
            .lock()
            .unwrap()
            .creator_places
            .insert(creator, places.clone());
        places
    }
}

// [HELPER FUNCTIONS]

/// Splits batch responses into found assets and private assets that need a place id.
/// Anything else with an error (like ids that aren't assets) is dropped.
fn split_batch_responses(
    responses: Vec<AssetBatchResponse>,
    found: &mut Vec<AssetBatchResponse>,
    private: &mut Vec<u64>,
) {
    for response in responses {
        let Some(errors) = &response.errors else {
            found.push(response);
            continue;
        };

        let asset_id = response
            .request_id
            .as_ref()
            .and_then(|id| id.parse::<u64>().ok());
        let is_private = errors.iter().any(|e| e.code == PRIVATE_ASSET_CODE);

        match asset_id {
            Some(asset_id) if is_private => private.push(asset_id),
            _ => debug!(
                "Skipping {:?}, not an available asset: {:?}",
                response.request_id, errors
            ),
        }
    }
}

/// Creates batch payloads from asset IDs.
fn create_batch_payloads(asset_ids: &[u64]) -> Vec<AssetBatchPayload> {
    asset_ids
        .iter()
        .map(|&asset_id| AssetBatchPayload {
            asset_id: Some(asset_id.to_string()),
            request_id: Some(asset_id.to_string()),
            ..Default::default()
        })
        .collect()
}
