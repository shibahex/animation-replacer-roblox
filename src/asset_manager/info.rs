use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderValue};
use roboat::{
    ClientBuilder, RoboatError,
    assetdelivery::{AssetBatchPayload, AssetBatchResponse},
};
use std::collections::{HashMap, HashSet};
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use crate::AssetUploader;

const DEFAULT_TIMEOUT_SECS: u64 = 10;
const BATCH_SIZE: usize = 250;
const MAX_FETCH_RETRIES: u32 = 9;
// Max times to sleep-and-retry when place_id fetch hits a rate limit.
// Without this cap, a single poisoned asset retries forever and keeps
// firing the global rate limiter for all other tasks.
const MAX_PLACE_ID_RATELIMIT_RETRIES: u32 = 5;

impl AssetUploader {
    /// Fetches animation metadata for multiple assets.
    pub async fn fetch_animation_assets(
        &self,
        asset_ids: Vec<u64>,
    ) -> anyhow::Result<Vec<AssetBatchResponse>> {
        let mut animations = Vec::new();

        for batch in asset_ids.chunks(BATCH_SIZE) {
            let batch_animations = fetch_single_batch(self, batch).await?;
            animations.extend(batch_animations);
        }

        Ok(animations)
    }

    /// Downloads file bytes from a URL with retry logic.
    pub async fn file_bytes_from_url(&self, url: String) -> Result<Bytes, RoboatError> {
        const MAX_RETRIES: usize = 3;
        const TIMEOUT_SECS: u64 = 15;

        let client = reqwest::Client::new();

        for attempt in 1..=MAX_RETRIES {
            let result =
                tokio::time::timeout(Duration::from_secs(TIMEOUT_SECS), client.get(&url).send())
                    .await;

            match result {
                Ok(Ok(response)) => {
                    return response.bytes().await.map_err(RoboatError::ReqwestError);
                }
                Ok(Err(e)) => {
                    if attempt == MAX_RETRIES {
                        return Err(RoboatError::ReqwestError(e));
                    }
                }
                Err(e) => {
                    error!("Getting file from animation url error: {:?}", e);
                    if attempt == MAX_RETRIES {
                        return Err(RoboatError::InternalServerError);
                    }
                }
            }
        }
        unreachable!("Loop should always return")
    }
}

// [BATCH FETCHING LOGIC]

/// Fetches a single batch of animation metadata with retry logic.
async fn fetch_single_batch(
    uploader: &AssetUploader,
    asset_ids: &[u64],
) -> anyhow::Result<Vec<AssetBatchResponse>> {
    // Success cache: creator_id -> place_id.
    // One group = one group_games API call, rest are HashMap hits.
    let mut place_id_cache: HashMap<u64, u64> = HashMap::new();

    // Failure cache: creator_ids that returned no games from the API.
    // Group 34981778 appears ~30 times. Without this: 30 API calls, all fail.
    // With this: 1 API call fails, 29 instant Err from HashSet::contains.
    let mut failed_creators: HashSet<u64> = HashSet::new();

    let init_place_id = get_initial_place_id(
        uploader,
        asset_ids,
        &mut place_id_cache,
        &mut failed_creators,
    )
    .await
    .unwrap_or(0);

    let mut success_responses = Vec::new();
    let mut failed_ids: HashMap<u64, Vec<u64>> = HashMap::new();

    attempt_batch_fetch(
        uploader,
        asset_ids,
        init_place_id,
        &mut success_responses,
        &mut failed_ids,
        &mut place_id_cache,
        &mut failed_creators,
    )
    .await?;

    let mut resolved = resolve_failed_assets(uploader, failed_ids).await;
    success_responses.append(&mut resolved);

    Ok(success_responses)
}

/// Attempts to fetch a batch of assets with a given place ID.
async fn attempt_batch_fetch(
    uploader: &AssetUploader,
    asset_ids: &[u64],
    place_id: u64,
    success_responses: &mut Vec<AssetBatchResponse>,
    failed_ids: &mut HashMap<u64, Vec<u64>>,
    place_id_cache: &mut HashMap<u64, u64>,
    failed_creators: &mut HashSet<u64>,
) -> anyhow::Result<()> {
    let mut attempts = 0;

    loop {
        let payload = create_batch_payloads(asset_ids);

        match check_asset_metadata(
            uploader,
            payload,
            place_id,
            Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        )
        .await
        {
            Ok(Some(responses)) => {
                process_batch_responses(
                    uploader,
                    responses,
                    success_responses,
                    failed_ids,
                    place_id_cache,
                    failed_creators,
                )
                .await;
                break;
            }
            Ok(None) => {
                warn!("No responses received from batch fetch");
                break;
            }
            Err(e) => {
                if !handle_fetch_error(uploader, &e, &mut attempts).await? {
                    break;
                }
            }
        }
    }

    Ok(())
}

/// Processes batch responses, separating successes and failures.
async fn process_batch_responses(
    uploader: &AssetUploader,
    responses: Vec<AssetBatchResponse>,
    success_responses: &mut Vec<AssetBatchResponse>,
    failed_ids: &mut HashMap<u64, Vec<u64>>,
    place_id_cache: &mut HashMap<u64, u64>,
    failed_creators: &mut HashSet<u64>,
) {
    for response in responses {
        if response.errors.is_none() {
            success_responses.push(response);
        } else if let Some(request_id) = response.request_id
            && let Ok(asset_id) = request_id.parse::<u64>()
        {
            handle_failed_asset(
                uploader,
                asset_id,
                failed_ids,
                place_id_cache,
                failed_creators,
            )
            .await;
        }
    }
}

/// Handles a failed asset by finding its place ID.
/// Uses place_id_cache (successes) and failed_creators (failures) to skip repeat API calls.
async fn handle_failed_asset(
    uploader: &AssetUploader,
    asset_id: u64,
    failed_ids: &mut HashMap<u64, Vec<u64>>,
    place_id_cache: &mut HashMap<u64, u64>,
    failed_creators: &mut HashSet<u64>,
) {
    match fetch_asset_place_id(
        uploader,
        asset_id,
        failed_ids,
        place_id_cache,
        failed_creators,
    )
    .await
    {
        Ok(place_id) => {
            info!("Found place_id: {} for asset: {}", place_id, asset_id);
            failed_ids.entry(place_id).or_default().push(asset_id);
        }
        Err(e) => {
            error!("Failed to get place_id for asset {}: {}", asset_id, e);
        }
    }
}

/// Handles errors during batch fetch with retry logic.
async fn handle_fetch_error(
    uploader: &AssetUploader,
    error: &anyhow::Error,
    attempts: &mut u32,
) -> anyhow::Result<bool> {
    *attempts += 1;

    if *attempts > MAX_FETCH_RETRIES {
        return Err(anyhow::anyhow!("Max retries exceeded"));
    }

    if let Some(roboat_error) = error.downcast_ref::<RoboatError>()
        && matches!(roboat_error, RoboatError::TooManyRequests)
    {
        let sleep_time = (*attempts as u64) * 30;
        info!("attempt_batch_fetch got ratelimit, waiting {}", sleep_time);
        uploader.rate_limiter.set_rate_limit(sleep_time).await;
        uploader.rate_limiter.wait_if_limited().await;
        return Ok(true);
    }

    if should_retry_error(error) {
        warn!(
            "Request failed, retrying (attempt {}/{}): {}",
            attempts, MAX_FETCH_RETRIES, error
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        return Ok(true);
    }

    Ok(false)
}

/// Resolves failed assets by retrying with correct place IDs.
async fn resolve_failed_assets(
    uploader: &AssetUploader,
    asset_and_places: HashMap<u64, Vec<u64>>,
) -> Vec<AssetBatchResponse> {
    let mut resolved_responses = Vec::new();

    for (place_id, vec_assets) in asset_and_places {
        let payload = create_batch_payloads(&vec_assets);

        match check_asset_metadata(uploader, payload, place_id, Duration::from_secs(5)).await {
            Ok(Some(responses)) => {
                for response in responses {
                    if response.errors.is_none() {
                        resolved_responses.push(response);
                    } else {
                        error!(
                            "Failed to resolve asset {:?} with place_id {}",
                            response.request_id, place_id
                        );
                    }
                }
            }
            Ok(None) => warn!("No response for place_id {}", place_id),
            Err(e) => error!("Error resolving assets for place_id {}: {:?}", place_id, e),
        }
    }

    resolved_responses
}

// [PLACE ID FETCHING]

/// Gets the initial place ID from the first valid asset.
async fn get_initial_place_id(
    uploader: &AssetUploader,
    asset_ids: &[u64],
    place_id_cache: &mut HashMap<u64, u64>,
    failed_creators: &mut HashSet<u64>,
) -> anyhow::Result<u64> {
    for &asset_id in asset_ids {
        match fetch_asset_place_id(
            uploader,
            asset_id,
            &mut HashMap::new(),
            place_id_cache,
            failed_creators,
        )
        .await
        {
            Ok(place_id) => {
                info!("Found place_id: {} for asset: {}", place_id, asset_id);
                return Ok(place_id);
            }
            Err(e) => {
                error!("Error getting place for asset {}: {:?}", asset_id, e);
            }
        }
    }

    Err(anyhow::anyhow!(
        "Could not find valid place ID for any asset"
    ))
}

/// Gets or fetches a place ID for an asset, using both caches.
///
/// `place_id_cache`: creator_id -> place_id for known-good creators.
/// `failed_creators`: creator_ids that returned no games. Instant Err, no API call.
///
/// Example - group 34981778 has no games:
///   Asset 1: get_asset_info -> group 34981778 -> group_games() -> Err -> insert into failed_creators
///   Asset 2: get_asset_info -> group 34981778 -> failed_creators.contains() -> instant Err, zero API calls
///   Asset 3..N: same as asset 2
async fn fetch_asset_place_id(
    uploader: &AssetUploader,
    asset_id: u64,
    failed_ids: &mut HashMap<u64, Vec<u64>>,
    place_id_cache: &mut HashMap<u64, u64>,
    failed_creators: &mut HashSet<u64>,
) -> anyhow::Result<u64> {
    // Check if any already-resolved place owns this asset_id
    for (&place_id, assets) in failed_ids.iter() {
        if assets.contains(&asset_id) {
            debug!(
                "found place_id {} in failed_ids cache for asset {}",
                place_id, asset_id
            );
            return Ok(place_id);
        }
    }

    let mut ratelimit_attempts: u32 = 0;

    loop {
        debug!("Fetching place_id for asset {}", asset_id);

        match get_place_id_from_asset(uploader, asset_id, place_id_cache, failed_creators).await {
            Ok(place_id) => {
                debug!("fetched place_id {} for asset {}", place_id, asset_id);
                return Ok(place_id);
            }
            Err(e) => {
                if let Some(RoboatError::TooManyRequests) = e.downcast_ref::<RoboatError>() {
                    ratelimit_attempts += 1;

                    // Cap rate-limit retries. Without this, a poisoned asset retries
                    // forever, firing the global limiter each cycle and blocking everything.
                    if ratelimit_attempts > MAX_PLACE_ID_RATELIMIT_RETRIES {
                        return Err(anyhow::anyhow!(
                            "Gave up on place_id for asset {} after {} rate-limit retries",
                            asset_id,
                            ratelimit_attempts
                        ));
                    }

                    let sleep_time = 4 + (ratelimit_attempts as u64 % 10);
                    warn!(
                        "Rate limited fetching place_id for asset {} (attempt {}/{}), \
                         sleeping {} seconds...",
                        asset_id, ratelimit_attempts, MAX_PLACE_ID_RATELIMIT_RETRIES, sleep_time
                    );
                    uploader.rate_limiter.set_rate_limit(sleep_time).await;
                    uploader.rate_limiter.wait_if_limited().await;
                    info!("Rate limit wait complete, retrying place_id fetch...");
                } else {
                    return Err(anyhow::anyhow!(
                        "Failed to get place_id after {} rate-limit retries: {}",
                        ratelimit_attempts,
                        e
                    ));
                }
            }
        }
    }
}

/// Fetches place_id for an asset by checking if it's owned by a user or group.
///
/// `place_id_cache` maps creator_id -> place_id (success cache).
/// `failed_creators` holds creator_ids whose games list returned empty (failure cache).
///
/// Without both caches - group 34981778 (30 assets, no games):
///   30 x get_asset_info() + 30 x group_games() = 60 API calls, all fail
/// With both caches:
///   30 x get_asset_info() + 1 x group_games() + 29 x HashSet::contains() = 31 calls
async fn get_place_id_from_asset(
    uploader: &AssetUploader,
    asset_id: u64,
    place_id_cache: &mut HashMap<u64, u64>,
    failed_creators: &mut HashSet<u64>,
) -> anyhow::Result<u64> {
    let client = ClientBuilder::new()
        .roblosecurity(uploader.roblosecurity.to_string())
        .build();

    let asset_info = client.get_asset_info(asset_id).await?;

    // Check if owned by user
    if let Some(user_id) = asset_info.creation_context.creator.user_id {
        let user_id_parsed = user_id
            .parse::<u64>()
            .map_err(|e| anyhow::anyhow!("Failed to parse user_id '{}': {}", user_id, e))?;

        // Failure cache hit: user has no games, skip API call entirely
        if failed_creators.contains(&user_id_parsed) {
            debug!("Failure cache hit: user {} has no games", user_id_parsed);
            return Err(anyhow::anyhow!(
                "Couldn't find place for user {}",
                user_id_parsed
            ));
        }

        // Success cache hit: no user_games API call needed
        if let Some(&cached_place_id) = place_id_cache.get(&user_id_parsed) {
            debug!(
                "Cache hit: user {} -> place_id {}",
                user_id_parsed, cached_place_id
            );
            return Ok(cached_place_id);
        }

        match get_user_place_id(user_id_parsed).await {
            Ok(place_id) => {
                place_id_cache.insert(user_id_parsed, place_id);
                return Ok(place_id);
            }
            Err(e) => {
                // Store in failure cache so future assets from same user skip the API
                failed_creators.insert(user_id_parsed);
                return Err(e);
            }
        }
    }

    // Check if owned by group
    if let Some(group_id) = asset_info.creation_context.creator.group_id {
        let group_id_parsed = group_id
            .parse::<u64>()
            .map_err(|e| anyhow::anyhow!("Failed to parse group_id '{}': {}", group_id, e))?;

        // Failure cache hit: group has no games, skip API call entirely
        if failed_creators.contains(&group_id_parsed) {
            debug!("Failure cache hit: group {} has no games", group_id_parsed);
            return Err(anyhow::anyhow!(
                "Couldn't find place for group {}",
                group_id_parsed
            ));
        }

        // Success cache hit: no group_games API call needed
        if let Some(&cached_place_id) = place_id_cache.get(&group_id_parsed) {
            debug!(
                "Cache hit: group {} -> place_id {}",
                group_id_parsed, cached_place_id
            );
            return Ok(cached_place_id);
        }

        match get_group_place_id(group_id_parsed).await {
            Ok(place_id) => {
                place_id_cache.insert(group_id_parsed, place_id);
                return Ok(place_id);
            }
            Err(e) => {
                // Store in failure cache so future assets from same group skip the API
                failed_creators.insert(group_id_parsed);
                return Err(e);
            }
        }
    }

    Err(anyhow::anyhow!(
        "No user_id or group_id found for asset {}",
        asset_id
    ))
}

/// Gets the root place ID for a user.
async fn get_user_place_id(user_id: u64) -> anyhow::Result<u64> {
    let client = ClientBuilder::new().build();
    let games_response = client.user_games(user_id, Some(true), None).await?;

    games_response
        .data
        .first()
        .map(|place| place.root_place.id)
        .ok_or_else(|| anyhow::anyhow!("Couldn't find place for user {}", user_id))
}

/// Gets the root place ID for a group.
async fn get_group_place_id(group_id: u64) -> anyhow::Result<u64> {
    let client = ClientBuilder::new().build();
    let games_response = client.group_games(group_id, Some(true), None).await?;

    games_response
        .data
        .first()
        .map(|place| place.root_place.id)
        .ok_or_else(|| anyhow::anyhow!("Couldn't find place for group {}", group_id))
}

// [ASSET METADATA API]

/// Checks asset metadata for up to 250 assets with a specific place_id header.
async fn check_asset_metadata(
    uploader: &AssetUploader,
    asset_ids: Vec<AssetBatchPayload>,
    place_id: u64,
    timeout_secs: Duration,
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
        .map_err(RoboatError::ReqwestError)?;

    let client = ClientBuilder::new()
        .roblosecurity(uploader.roblosecurity.clone())
        .reqwest_client(timeout_client)
        .build();

    match client.post_asset_metadata_batch(asset_ids).await {
        Ok(x) => Ok(Some(x)),
        Err(e) => Err(e.into()),
    }
}

// [HELPER FUNCTIONS]

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

/// Determines if an error should trigger a retry.
fn should_retry_error(error: &anyhow::Error) -> bool {
    if let Some(roboat_error) = error.downcast_ref::<RoboatError>() {
        matches!(roboat_error, RoboatError::MalformedResponse)
            || matches!(roboat_error, RoboatError::ReqwestError(e) if e.is_timeout())
    } else {
        false
    }
}
