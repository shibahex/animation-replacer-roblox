use bytes::Bytes;
use roboat::ClientBuilder;
use roboat::RoboatError;
use roboat::assetdelivery::AssetBatchResponse;
use roboat::catalog::CreatorType;
use roboat::ide::ide_types::NewAnimation;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct AnimationUploader {
    pub roblosecurity: String,
}

#[derive(Debug)]
pub struct AnimationWithPlace {
    pub animation: AssetBatchResponse,
    pub place_id: u64,
}

pub struct OwnerInfo {
    pub owner_id: u64,
    pub owner_type: CreatorType,
}

impl AnimationUploader {
    /// Creates a new AnimationUploader with a roblosecurity cookie.
    pub fn new(roblosecurity: String) -> Self {
        Self { roblosecurity }
    }

    /// Uploads animation data to Roblox.
    ///
    /// * Parameters
    /// Animation_Data: Bytes
    /// Group Id to upload to (Option)
    ///
    /// * Returns
    /// New Asset Id (Sucess)
    /// RoboatError (Failed)
    ///
    /// * Examples
    ///
    /// ```rust
    /// let uploader = AnimationUploader::new(cookie);
    /// let data = std::fs::read("animation.rbxm")?.into();
    /// let id = uploader.upload_animation(data, Some(123456)).await?;
    /// ```
    pub async fn upload_animation(
        &self,
        animation_data: Bytes,
        group_id: Option<u64>,
    ) -> Result<String, RoboatError> {
        let client = ClientBuilder::new()
            .roblosecurity(self.roblosecurity.clone())
            .build();

        let animation = NewAnimation {
            group_id: group_id,
            name: "reuploaded_animation".to_string(),
            description: "This is a example".to_string(),
            animation_data,
        };

        let new_asset_id_string = client.upload_new_animation(animation).await?;
        Ok(new_asset_id_string)
    }

    /// Reuploads animations concurrently with semaphore limiting.
    ///
    /// * Notes
    /// Uses Semaphore for multiproccessing, default it set at 5 semphores
    ///
    ///
    /// # Example
    /// ```rust
    /// let animtions: Vec<AssetBatchResponse> = Vec::New(EXAMPLE)
    /// let uploader = Arc::new(AnimationUploader::new(cookie));
    /// let mapping = uploader.reupload_all_animations(animations, Some(group_id)).await?;
    /// ```
    pub async fn reupload_all_animations(
        self: Arc<Self>,
        animations: Vec<AssetBatchResponse>,
        group_id: Option<u64>,
        task_count: Option<u64>,
    ) -> Result<HashMap<String, String>, RoboatError> {
        let rate_limit_until = Arc::new(tokio::sync::Mutex::new(None::<tokio::time::Instant>));
        let max_concurrent_tasks = task_count.unwrap_or(500);

        let semaphore = Arc::new(Semaphore::new(max_concurrent_tasks as usize));
        let mut tasks = Vec::new();
        let total_animations = animations.len();

        for (index, animation) in animations.into_iter().enumerate() {
            let location_string = animation
                .locations
                .as_ref()
                .and_then(|locs| locs.first())
                .and_then(|loc| loc.location.as_ref());

            if let Some(location) = location_string {
                let semaphore = semaphore.clone();
                let self_arc = Arc::clone(&self);
                let location = location.to_string();
                let request_id = animation.request_id.clone();
                let group_id = group_id.clone();
                let rate_limit_until = rate_limit_until.clone();

                let task = tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let animation_file = self_arc.file_bytes_from_url(location).await?;

                    // Retry logic for upload_animation
                    let max_upload_retries: usize = 5;
                    let mut last_error = None;

                    // Check if we should wait for rate limit
                    loop {
                        let until = *rate_limit_until.lock().await;
                        if let Some(wake_time) = until {
                            if tokio::time::Instant::now() < wake_time {
                                tokio::time::sleep_until(wake_time).await;
                                continue;
                            }
                        }
                        break;
                    }

                    for attempt in 1..=max_upload_retries {
                        match self_arc
                            .upload_animation(animation_file.clone(), group_id)
                            .await
                        {
                            Ok(new_animation_id) => {
                                println!(
                                    "Success uploading animation {}/{} ({} remaining)",
                                    index + 1,
                                    total_animations,
                                    total_animations - (index + 1),
                                );
                                return Ok::<_, RoboatError>((request_id, new_animation_id));
                            }

                            Err(e) => {
                                eprintln!(
                                    "Upload attempt {}/{} failed for animation {}: {}",
                                    attempt,
                                    max_upload_retries,
                                    index + 1,
                                    e
                                );

                                match e {
                                    RoboatError::TooManyRequests
                                    | RoboatError::InternalServerError => {
                                        let time_sleep = (attempt as u64) * 30;
                                        println!(
                                            "{:?} hit: All threads sleeping {} seconds",
                                            e, time_sleep
                                        );

                                        let wake_time = tokio::time::Instant::now()
                                            + tokio::time::Duration::from_secs(time_sleep);

                                        // Set the global rate limit
                                        *rate_limit_until.lock().await = Some(wake_time);

                                        tokio::time::sleep_until(wake_time).await;
                                    }
                                    _ => {}
                                }

                                last_error = Some(e);

                                // Don't sleep on the last attempt
                                if attempt < max_upload_retries {
                                    tokio::time::sleep(tokio::time::Duration::from_millis(1000))
                                        .await;
                                }
                            }
                        }
                    }

                    // All retries failed
                    Err(last_error.unwrap())
                });

                tasks.push(task);
            }
        }

        let mut animation_hashmap = HashMap::new();
        let mut errors = Vec::new();
        let total_tasks = tasks.len();

        for task in tasks {
            match task.await {
                // Task completed successfully with a result and request_id exists
                Ok(Ok((Some(request_id), new_animation_id))) => {
                    animation_hashmap.insert(request_id, new_animation_id);
                }

                // Handle case where animation_id is None
                Ok(Ok((None, _))) => {
                    eprintln!("Warning: Animation uploader success but no animation_id available");
                }

                // Task completed but your function returned an error
                Ok(Err(e)) => {
                    if matches!(e, RoboatError::BadRequest) {
                        eprintln!(
                            "Animation Upload API failed to respond with errors; Cookie cannot publish animations. \nwith group uploading; make sure the cookie has perms to ALL Asset and Experience permissions\n"
                        )
                    } else {
                        eprintln!("Animation upload failed: {}", e);
                    }
                    errors.push(e);
                }

                // Task panicked or was cancelled
                Err(join_error) => {
                    eprintln!("Task failed to execute: {}", join_error);
                    // Just log the error and continue - don't fail the entire batch
                }
            }
        }

        // Handle collected errors
        if !errors.is_empty() {
            eprintln!(
                "Some uploads failed: {} out of {} tasks\nERROR INFO: {:?}",
                errors.len(),
                total_tasks,
                errors
            );
        }

        Ok(animation_hashmap)
    }

    ///  Gets all the animation file data to re-upload them
    /// * Notes
    /// This func uses caching and hashmaps to handle needing place-id to download assets
    pub async fn fetch_animation_assets(
        &self,
        asset_ids: Vec<u64>,
    ) -> anyhow::Result<Vec<AssetBatchResponse>> {
        let mut animations: Vec<AssetBatchResponse> = Vec::new();
        let batch_size = 250;

        for batch in asset_ids.chunks(batch_size) {
            let batch_animations = self.fetch_batch_with_retry(batch).await?;
            animations.extend(batch_animations);
        }

        Ok(animations)
    }
}

mod internal {
    use std::{collections::HashMap, time::Duration};
    use tokio::time;

    use roboat::{
        RoboatError,
        assetdelivery::{AssetBatchPayload, AssetBatchResponse},
    };

    use crate::AnimationUploader;

    impl AnimationUploader {
        /// Fetches asset metadata for a batch of asset IDs with automatic retry logic and 403 error handling.
        ///
        /// This function attempts to retrieve asset metadata for the provided asset IDs, handling
        /// error conditions specifically manages 403 permission
        /// errors by dynamically discovering and using place IDs
        ///
        /// # Parameters
        /// * `asset_ids` - A slice of asset IDs to fetch metadata for (typically up to 250 per batch)
        ///
        /// # Returns
        /// * `Ok(Vec<AssetBatchResponse>)` - Successfully fetched and filtered animation assets
        /// * `Err(anyhow::Error)` - Failed after all retry attempts or encountered unrecoverable error
        ///
        /// # Retry Logic
        /// - **Max Retries**: 9 attempts with exponential timeout increase
        /// - **403 Errors**: Automatically discovers place_id from first 403 error and retries
        /// - **Network Errors**: Retries with increased timeout for reqwest timeout/malformed response errors
        /// - **Timeout Progression**: Starts at 4 seconds, increases by 1 second per retry attempt
        ///
        /// # Error Handling
        /// - **403 Forbidden**: Extracts place_id from the failing asset and retries the entire batch
        /// - **Timeout/Network**: Implements backoff strategy with 2-second sleep between attempts
        /// - **Other Errors**: Fails immediately without retry
        ///
        /// # Examples
        /// ```rust
        /// let mut cache = HashMap::new();
        /// let mut place_id = None;
        /// let asset_ids = vec![123456, 789012, 345678];
        ///
        /// let animations = uploader
        ///     .fetch_batch_with_retry(&asset_ids, &mut cache, &mut place_id)
        ///     .await?;
        /// ```
        pub(super) async fn fetch_batch_with_retry(
            &self,
            asset_ids: &[u64],
        ) -> anyhow::Result<Vec<AssetBatchResponse>> {
            use tokio::time::{Duration, sleep};

            // NOTE:
            // 1. Try the asset_ids once.
            // 2. Make a hashmap for failed ids
            // 3. Resolve the place_id and have value:key as asset_id:place_id
            // 4. After scanning all the responses resolve the errors
            // (Retry the places with the place found once and if it doesnt work dont resolve it)

            // Have placeid: vec[asset_ids] hashmap
            let mut failed_ids: HashMap<u64, Vec<u64>> = HashMap::new();

            // get place id
            let init_place_id = self.get_initial_place(asset_ids).await.unwrap_or(0);
            //
            let mut sucess_responses: Vec<AssetBatchResponse> = Vec::new();
            const MAX_RETRIES: u32 = 9;
            let mut attempts = 0;

            loop {
                let initial_payload = self.create_batch_payloads(asset_ids);

                match self
                    .check_asset_metadata(initial_payload, init_place_id, Duration::from_secs(10))
                    .await
                {
                    Ok(Some(responses)) => {
                        // look for success and fails
                        for response in responses {
                            if response.errors.is_none() {
                                // make asset_id a u64
                                println!("sucessfully fetched animation details");
                                sucess_responses.push(response);
                            } else if response.errors.is_some() {
                                // println!("errors is {:?}", response.errors);
                                // if the response has error then map it in failed_ids
                                let request_id = response.request_id;
                                // make asset_id a u64
                                if let Some(asset_id) =
                                    request_id.and_then(|s| s.parse::<u64>().ok())
                                {
                                    match self
                                        .get_or_fetch_place_id(asset_id, &mut failed_ids)
                                        .await
                                    {
                                        Ok(place_id) => {
                                            // Make a key of the place_id or if its there make the list
                                            // bigger with the asset_id
                                            println!(
                                                "Found place_id: {} for asset: {}",
                                                place_id, asset_id
                                            );

                                            failed_ids
                                                .entry(place_id)
                                                .or_insert_with(Vec::new)
                                                .push(asset_id);
                                        }
                                        Err(e) => {
                                            println!("failed to get place id: {}", e);
                                        }
                                    }
                                }
                            } else {
                                println!("request_id is None request: {:?} BREAKING", response);
                            }
                        }
                        break;
                    }
                    Ok(None) => {
                        println!("got no responses");
                        break;
                    }
                    // TODO: make refactor should_retry code and add both ratelimit logic and retry
                    // logic more gracefully, because the both unwrap for roboat errors
                    Err(e) => {
                        if attempts > MAX_RETRIES {
                            return Err(anyhow::anyhow!(
                                "Couldn't fetch metadata; Max retries was exceeded"
                            ));
                        }
                        println!("error checking asset metadata: {}", e);
                        attempts += 1;
                        if let Some(roboat_error) = e.downcast_ref::<RoboatError>() {
                            match roboat_error {
                                RoboatError::TooManyRequests => {
                                    let time_sleep: u64 = (attempts as u64) * 30;
                                    println!(
                                        "Ratelimited by fetching asset metadata sleeping {}, (Could be caused by blocked VPN or proxy)",
                                        time_sleep
                                    );
                                    time::sleep(Duration::from_secs(time_sleep)).await;

                                    continue;
                                }
                                _ => {}
                            }
                        }

                        if self.should_retry(&e, attempts, MAX_RETRIES) {
                            // attempts += 1;
                            println!(
                                "Request failed, retrying with higher timeout: attempts {}/{} ({})",
                                attempts, MAX_RETRIES, e
                            );

                            sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                        break;
                        // break; // or handle error as appropriate
                    }
                }
            }

            // TODO: resolve places
            let mut resolved_responses = self.resolve_assets_with_places(failed_ids).await;
            sucess_responses.append(&mut resolved_responses);
            return Ok(sucess_responses);
        }

        /// Returns Place Id (String) and asset_id (u64)
        pub(super) async fn get_initial_place(&self, asset_ids: &[u64]) -> anyhow::Result<u64> {
            let mut empty_map: HashMap<u64, Vec<u64>> = HashMap::new();
            for asset_id in asset_ids {
                match self.get_or_fetch_place_id(*asset_id, &mut empty_map).await {
                    Ok(place_id) => {
                        return Ok(place_id);
                    }
                    Err(e) => {
                        eprintln!("Error getting place for asset {}: {:?}", asset_id, e);
                        continue;
                    }
                }
            }
            Err(anyhow::anyhow!(
                "Could not find valid place ID for any asset"
            ))
        }

        /// Helper func for fetch_batch_with_retry to resolve all the 403 errors that have to do
        /// with place_id not being in headers
        pub(super) async fn resolve_assets_with_places(
            &self,
            asset_and_places: HashMap<u64, Vec<u64>>,
        ) -> Vec<AssetBatchResponse> {
            use tokio::time::Duration;
            let mut resolved_responses: Vec<AssetBatchResponse> = Vec::new();

            for (place_id, vec_assets) in asset_and_places {
                let payload = self.create_batch_payloads(&vec_assets);

                match self
                    .check_asset_metadata(payload, place_id, Duration::from_secs(5))
                    .await
                {
                    Ok(Some(responses)) => {
                        // debug
                        for response in responses {
                            if response.errors.is_none() {
                                println!("sucessfully resolved error: {:?}", response.request_id);
                                resolved_responses.push(response);
                            } else {
                                eprintln!(
                                    "error not resolved for {:?} after using place_id {:?} in headers \n response is {:?}",
                                    response.request_id, place_id, response
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        println!("got no response");
                    }
                    Err(e) => {
                        eprintln!("{:?}", e);
                    }
                }
            }
            return resolved_responses;
        }

        /// Fetches the assets place id by calling the internal place_id func. will skip this
        /// function if place_id was already found
        pub(super) async fn get_or_fetch_place_id(
            &self,
            asset_id: u64,
            cached_places: &mut HashMap<u64, Vec<u64>>, // place_id -> asset_ids
        ) -> anyhow::Result<u64> {
            // Try to find if asset_id is already recorded
            for (place_id, assets) in cached_places.iter() {
                if assets.contains(&asset_id) {
                    println!(
                        "Found place_id {} in cache for asset {}",
                        place_id, asset_id
                    );
                    return Ok(*place_id);
                }
            }

            loop {
                match self.place_id(asset_id, cached_places).await {
                    Ok(place_id) => {
                        cached_places.entry(place_id).or_default().push(asset_id);
                        return Ok(place_id);
                    }
                    Err(e) => {
                        if let Some(RoboatError::TooManyRequests) = e.downcast_ref::<RoboatError>()
                        {
                            println!(
                                "place_id fetching got ratelimited waiting 4 seconds then retrying.."
                            );
                            time::sleep(Duration::from_secs(4)).await;
                        } else {
                            return Err(anyhow::anyhow!(
                                "got error other than too many requests: {}",
                                e
                            ));
                        }
                    }
                }
            }
            // .with_context(|| format!("Failed to get place id for asset {}", asset_id))?;
        }

        pub(super) fn should_retry(
            &self,
            error: &anyhow::Error,
            attempts: u32,
            max_retries: u32,
        ) -> bool {
            use roboat::RoboatError;
            if attempts >= max_retries {
                return false;
            }

            if let Some(roboat_error) = error.downcast_ref::<RoboatError>() {
                match roboat_error {
                    RoboatError::ReqwestError(reqwest_err) if reqwest_err.is_timeout() => true,
                    RoboatError::MalformedResponse => true,
                    _ => false,
                }
            } else {
                false
            }
        }
        ///
        /// Takes in a vector of asset_ids then formats them in the payload that roblox expects
        pub(super) fn create_batch_payloads(&self, asset_ids: &[u64]) -> Vec<AssetBatchPayload> {
            asset_ids
                .iter()
                .map(|&asset_id| AssetBatchPayload {
                    asset_id: Some(asset_id.to_string()),
                    request_id: Some(asset_id.to_string()),
                    ..Default::default()
                })
                .collect()
        }
    }
}
