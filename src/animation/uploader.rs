use bytes::Bytes;
use roboat::ClientBuilder;
use roboat::RoboatError;
use roboat::assetdelivery::{AssetBatchPayload, AssetBatchResponse};
use roboat::ide::ide_types::NewAnimation;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::Duration;

use super::tasks::{RateLimiter, collect_upload_results, spawn_upload_tasks};

const DEFAULT_CONCURRENT_TASKS: u64 = 50;
const DEFAULT_TIMEOUT_SECS: u64 = 10;
const BATCH_SIZE: usize = 250;
const MAX_FETCH_RETRIES: u32 = 9;

pub struct AnimationUploader {
    pub roblosecurity: String,
    pub(super) rate_limiter: Arc<RateLimiter>,
}

impl AnimationUploader {
    /// Creates a new AnimationUploader with a roblosecurity cookie.
    pub fn new(roblosecurity: String) -> Self {
        Self {
            roblosecurity,
            rate_limiter: Arc::new(RateLimiter::new()),
        }
    }

    /// Uploads a single animation to Roblox.
    pub async fn upload_animation(
        &self,
        animation_data: Bytes,
        group_id: Option<u64>,
    ) -> Result<String, RoboatError> {
        let client = ClientBuilder::new()
            .roblosecurity(self.roblosecurity.clone())
            .build();

        let animation = NewAnimation {
            group_id,
            name: "reuploaded_animation".to_string(),
            description: "This is a example".to_string(),
            animation_data,
        };

        client.upload_new_animation(animation).await
    }

    /// Fetches animation metadata for multiple assets.
    pub async fn fetch_animation_assets(
        &self,
        asset_ids: Vec<u64>,
    ) -> anyhow::Result<Vec<AssetBatchResponse>> {
        let mut animations = Vec::new();

        for batch in asset_ids.chunks(BATCH_SIZE) {
            let batch_animations = self.fetch_single_batch(batch).await?;
            animations.extend(batch_animations);
        }

        Ok(animations)
    }

    /// Reuploads multiple animations concurrently.
    pub async fn reupload_all_animations(
        self: Arc<Self>,
        animations: Vec<AssetBatchResponse>,
        group_id: Option<u64>,
        task_count: Option<u64>,
    ) -> Result<HashMap<String, String>, RoboatError> {
        let max_concurrent_tasks = task_count.unwrap_or(DEFAULT_CONCURRENT_TASKS);
        let total_animations = animations.len();

        let tasks = spawn_upload_tasks(
            self.clone(),
            animations,
            group_id,
            max_concurrent_tasks,
            total_animations,
        );

        collect_upload_results(tasks).await
    }

    /// Fetches a single batch of animation metadata with retry logic.
    async fn fetch_single_batch(
        &self,
        asset_ids: &[u64],
    ) -> anyhow::Result<Vec<AssetBatchResponse>> {
        let init_place_id = get_initial_place_id(self, asset_ids).await.unwrap_or(0);
        let mut success_responses = Vec::new();
        let mut failed_ids: HashMap<u64, Vec<u64>> = HashMap::new();

        // Try initial fetch
        attempt_batch_fetch(
            self,
            asset_ids,
            init_place_id,
            &mut success_responses,
            &mut failed_ids,
        )
        .await?;

        // Resolve failed fetches with correct place IDs
        let mut resolved = resolve_failed_assets(self, failed_ids).await;
        success_responses.append(&mut resolved);

        Ok(success_responses)
    }
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

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

        match uploader
            .check_asset_metadata(payload, place_id, Duration::from_secs(DEFAULT_TIMEOUT_SECS))
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

        match uploader
            .check_asset_metadata(payload, place_id, Duration::from_secs(5))
            .await
        {
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

        match uploader.place_id(asset_id, cached_places).await {
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
