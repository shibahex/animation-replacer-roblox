use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderValue};
use roboat::{
    ClientBuilder, RoboatError,
    assetdelivery::{AssetBatchPayload, AssetBatchResponse},
};
use std::collections::HashMap;
use tokio::time::Duration;

use crate::AnimationUploader;

const DEFAULT_TIMEOUT_SECS: u64 = 10;
const BATCH_SIZE: usize = 250;
const MAX_FETCH_RETRIES: u32 = 9;

impl AnimationUploader {
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
                    eprintln!("Getting file from animation url error: {:?}", e);
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
    uploader: &AnimationUploader,
    asset_ids: &[u64],
) -> anyhow::Result<Vec<AssetBatchResponse>> {
    let init_place_id = get_initial_place_id(uploader, asset_ids).await.unwrap_or(0);
    let mut success_responses = Vec::new();
    let mut failed_ids: HashMap<u64, Vec<u64>> = HashMap::new();

    // Try initial fetch
    attempt_batch_fetch(
        uploader,
        asset_ids,
        init_place_id,
        &mut success_responses,
        &mut failed_ids,
    )
    .await?;

    // Resolve failed fetches with correct place IDs
    let mut resolved = resolve_failed_assets(uploader, failed_ids).await;
    success_responses.append(&mut resolved);

    Ok(success_responses)
}

/// Attempts to fetch a batch of assets with a given place ID.
async fn attempt_batch_fetch(
    uploader: &AnimationUploader,
    asset_ids: &[u64],
    place_id: u64,
    success_responses: &mut Vec<AssetBatchResponse>,
    failed_ids: &mut HashMap<u64, Vec<u64>>,
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
                process_batch_responses(uploader, responses, success_responses, failed_ids).await;
                break;
            }
            Ok(None) => {
                println!("No responses received from batch fetch");
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
    uploader: &AnimationUploader,
    responses: Vec<AssetBatchResponse>,
    success_responses: &mut Vec<AssetBatchResponse>,
    failed_ids: &mut HashMap<u64, Vec<u64>>,
) {
    for response in responses {
        if response.errors.is_none() {
            success_responses.push(response);
        } else if let Some(request_id) = response.request_id
            && let Ok(asset_id) = request_id.parse::<u64>()
        {
            handle_failed_asset(uploader, asset_id, failed_ids).await;
        }
    }
}

/// Handles a failed asset by finding its place ID.
async fn handle_failed_asset(
    uploader: &AnimationUploader,
    asset_id: u64,
    failed_ids: &mut HashMap<u64, Vec<u64>>,
) {
    match fetch_asset_place_id(uploader, asset_id, failed_ids).await {
        Ok(place_id) => {
            println!("Found place_id: {} for asset: {}", place_id, asset_id);
            failed_ids.entry(place_id).or_default().push(asset_id);
        }
        Err(e) => {
            eprintln!("Failed to get place_id for asset {}: {}", asset_id, e);
        }
    }
}

/// Handles errors during batch fetch with retry logic.
async fn handle_fetch_error(
    uploader: &AnimationUploader,
    error: &anyhow::Error,
    attempts: &mut u32,
) -> anyhow::Result<bool> {
    *attempts += 1;

    if *attempts > MAX_FETCH_RETRIES {
        return Err(anyhow::anyhow!("Max retries exceeded"));
    }

    // Handle rate limiting - affects all concurrent operations
    if let Some(roboat_error) = error.downcast_ref::<RoboatError>()
        && matches!(roboat_error, RoboatError::TooManyRequests)
    {
        let sleep_time = (*attempts as u64) * 30;
        uploader.rate_limiter.set_rate_limit(sleep_time).await;
        uploader.rate_limiter.wait_if_limited().await;
        return Ok(true);
    }

    // Handle retryable errors
    if should_retry_error(error) {
        println!(
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
    uploader: &AnimationUploader,
    asset_and_places: HashMap<u64, Vec<u64>>,
) -> Vec<AssetBatchResponse> {
    let mut resolved_responses = Vec::new();

    for (place_id, vec_assets) in asset_and_places {
        let payload = create_batch_payloads(&vec_assets);

        match check_asset_metadata(uploader, payload, place_id, Duration::from_secs(5)).await {
            Ok(Some(responses)) => {
                for response in responses {
                    if response.errors.is_none() {
                        println!("Successfully resolved asset: {:?}", response.request_id);
                        resolved_responses.push(response);
                    } else {
                        eprintln!(
                            "Failed to resolve asset {:?} with place_id {}",
                            response.request_id, place_id
                        );
                    }
                }
            }
            Ok(None) => println!("No response for place_id {}", place_id),
            Err(e) => eprintln!("Error resolving assets for place_id {}: {:?}", place_id, e),
        }
    }

    resolved_responses
}

// [PLACE ID FETCHING]

/// Gets the initial place ID from the first valid asset.
async fn get_initial_place_id(
    uploader: &AnimationUploader,
    asset_ids: &[u64],
) -> anyhow::Result<u64> {
    let mut empty_map = HashMap::new();

    for &asset_id in asset_ids {
        match fetch_asset_place_id(uploader, asset_id, &mut empty_map).await {
            Ok(place_id) => return Ok(place_id),
            Err(e) => {
                eprintln!("Error getting place for asset {}: {:?}", asset_id, e);
            }
        }
    }

    Err(anyhow::anyhow!(
        "Could not find valid place ID for any asset"
    ))
}

/// Gets or fetches a place ID for an asset, using cache when available.
async fn fetch_asset_place_id(
    uploader: &AnimationUploader,
    asset_id: u64,
    cached_places: &mut HashMap<u64, Vec<u64>>,
) -> anyhow::Result<u64> {
    // Check cache first
    for (&place_id, assets) in cached_places.iter() {
        if assets.contains(&asset_id) {
            println!(
                "Found place_id {} in cache for asset {}",
                place_id, asset_id
            );
            return Ok(place_id);
        }
    }

    // Fetch with infinite retry logic for rate limits
    let mut attempt = 0;
    loop {
        attempt += 1;
        println!(
            "Attempt {} to fetch place_id for asset {}",
            attempt, asset_id
        );

        match get_place_id_from_asset(uploader, asset_id, cached_places).await {
            Ok(place_id) => {
                cached_places.entry(place_id).or_default().push(asset_id);
                println!(
                    "Successfully fetched place_id {} for asset {} after {} attempts",
                    place_id, asset_id, attempt
                );
                return Ok(place_id);
            }
            Err(e) => {
                if let Some(RoboatError::TooManyRequests) = e.downcast_ref::<RoboatError>() {
                    let sleep_time = 4 + (attempt % 10);
                    println!(
                        "Rate limited while fetching place_id (attempt {}), \
                         setting global rate limit for {} seconds...",
                        attempt, sleep_time
                    );
                    uploader.rate_limiter.set_rate_limit(sleep_time).await;
                    uploader.rate_limiter.wait_if_limited().await;
                    println!("Rate limit wait complete, retrying place_id fetch...");
                } else {
                    return Err(anyhow::anyhow!(
                        "Failed to get place_id after {} attempts: {}",
                        attempt,
                        e
                    ));
                }
            }
        }
    }
}

/// Fetches place_id for an asset by checking if it's owned by a user or group.
async fn get_place_id_from_asset(
    uploader: &AnimationUploader,
    asset_id: u64,
    cached_places: &mut HashMap<u64, Vec<u64>>,
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

        let place_id = get_user_place_id(user_id_parsed).await?;
        cached_places.entry(place_id).or_default().push(asset_id);
        return Ok(place_id);
    }

    // Check if owned by group
    if let Some(group_id) = asset_info.creation_context.creator.group_id {
        let group_id_parsed = group_id
            .parse::<u64>()
            .map_err(|e| anyhow::anyhow!("Failed to parse group_id '{}': {}", group_id, e))?;

        let place_id = get_group_place_id(group_id_parsed).await?;
        cached_places.entry(place_id).or_default().push(asset_id);
        return Ok(place_id);
    }

    Err(anyhow::anyhow!(
        "No user_id or group_id found for asset {}",
        asset_id
    ))
}

/// Gets the root place ID for a user.
async fn get_user_place_id(user_id: u64) -> anyhow::Result<u64> {
    let client = ClientBuilder::new().build();
    let games_response = client.user_games(user_id).await?;

    games_response
        .data
        .first()
        .map(|place| place.root_place.id)
        .ok_or_else(|| anyhow::anyhow!("Couldn't find place for user {}", user_id))
}

/// Gets the root place ID for a group.
async fn get_group_place_id(group_id: u64) -> anyhow::Result<u64> {
    let client = ClientBuilder::new().build();
    let games_response = client.group_games(group_id).await?;

    games_response
        .data
        .first()
        .map(|place| place.root_place.id)
        .ok_or_else(|| anyhow::anyhow!("Couldn't find place for group {}", group_id))
}

// [ASSET METADATA API]

/// Checks asset metadata for up to 250 assets with a specific place_id header.
async fn check_asset_metadata(
    uploader: &AnimationUploader,
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
