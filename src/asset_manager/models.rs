use super::tasks::{UploadContext, asset_location, collect_upload_results};
use super::uploader::is_reuploadable;
use crate::StudioParser;
use crate::studio::file_format::{read_dom, write_dom};
use bytes::Bytes;
use roboat::assetdelivery::AssetBatchResponse;
use roboat::catalog::AssetType;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tracing::{error, info, warn};

/// How deep to follow models inside models. Stops loops and huge chains of models.
const MAX_MODEL_DEPTH: usize = 5;

type ModelFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>>;

impl UploadContext {
    /// Reuploads models. Before a model is uploaded, the assets inside it (animations, sounds,
    /// images, other models, ...) are reuploaded and their ids are replaced in the model.
    ///
    /// `asset_mapping` holds old id -> new id for everything already uploaded, so assets that
    /// are used in the place and inside a model are only uploaded once. New uploads are added to it.
    pub async fn reupload_models(
        &self,
        models: Vec<AssetBatchResponse>,
        asset_mapping: &mut HashMap<String, String>,
    ) {
        let total_models = models.len();
        let mut visited: HashSet<String> = HashSet::new();

        // Models count their own progress, separate from the other assets
        let model_ctx = UploadContext {
            total_assets: total_models,
            completed: Arc::new(AtomicUsize::new(0)),
            ..self.clone()
        };

        for (index, model) in models.into_iter().enumerate() {
            let model_id = model.request_id.clone().unwrap_or_default();
            info!(
                "Reuploading model {} ({}/{})",
                model_id,
                index + 1,
                total_models
            );

            if let Err(e) = model_ctx
                .reupload_model(model, asset_mapping, &mut visited, 0)
                .await
            {
                error!("Failed to reupload model {}: {:?}", model_id, e);
            }
        }
    }

    /// Reuploads one model, after reuploading the assets inside it.
    fn reupload_model<'a>(
        &'a self,
        model: AssetBatchResponse,
        asset_mapping: &'a mut HashMap<String, String>,
        visited: &'a mut HashSet<String>,
        depth: usize,
    ) -> ModelFuture<'a> {
        Box::pin(async move {
            let model_id = model
                .request_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Model has no request id"))?;

            // Already uploaded, or a model that contains itself somewhere down the chain
            if asset_mapping.contains_key(&model_id) || !visited.insert(model_id.clone()) {
                return Ok(());
            }

            let location = asset_location(&model)
                .ok_or_else(|| anyhow::anyhow!("No download location for model {}", model_id))?;
            let model_data = self.uploader.file_bytes_from_url(location).await?;

            let model_data = self
                .reupload_assets_in_model(&model_id, model_data, asset_mapping, visited, depth)
                .await;

            let new_model_id = self
                .upload_asset_with_retry(model_data, AssetType::Model, model_id.clone())
                .await?;

            info!("Model {} reuploaded as {}", model_id, new_model_id);
            asset_mapping.insert(model_id, new_model_id);
            Ok(())
        })
    }

    /// Reuploads the assets inside a model and returns the model with their ids replaced.
    ///
    /// If the model can't be read, the original model is returned
    /// and only the model itself gets reuploaded.
    async fn reupload_assets_in_model(
        &self,
        model_id: &str,
        model_data: Bytes,
        asset_mapping: &mut HashMap<String, String>,
        visited: &mut HashSet<String>,
        depth: usize,
    ) -> Bytes {
        let dom = match read_dom(&model_data) {
            Ok(dom) => dom,
            Err(e) => {
                warn!(
                    "Couldn't read model {}, the assets inside it won't be reuploaded: {}",
                    model_id, e
                );
                return model_data;
            }
        };

        let mut parser = StudioParser {
            roblosecurity: Some(self.uploader.roblosecurity.clone()),
            api_key: Some(self.uploader.api_key.clone()),
            dom,
        };

        // Name the assets inside after their instances in the model, and the model itself
        // after its top instance if the place didn't give it a name
        let mut names = parser.asset_names();
        if let (Ok(id), Some(top)) = (
            model_id.parse::<u64>(),
            parser
                .dom
                .root()
                .children()
                .first()
                .and_then(|r| parser.dom.get_by_ref(*r)),
        ) {
            names.insert(id, top.name.clone());
        }
        self.uploader.add_asset_names(names);

        // Every asset id in the model, that hasn't been uploaded yet
        let mut inner_ids = parser.asset_ids_in_instances();
        inner_ids.extend(parser.asset_ids_in_scripts(false));
        inner_ids.sort();
        inner_ids.dedup();
        inner_ids.retain(|id| !asset_mapping.contains_key(&id.to_string()));

        if inner_ids.is_empty() {
            return model_data;
        }

        info!(
            "Model {} has {} assets inside it to reupload",
            model_id,
            inner_ids.len()
        );

        let inner_assets = match self.uploader.fetch_animation_assets(inner_ids).await {
            Ok(assets) => assets,
            Err(e) => {
                warn!(
                    "Couldn't get the assets inside model {}, only the model will be reuploaded: {:?}",
                    model_id, e
                );
                return model_data;
            }
        };

        let (inner_models, inner_assets): (Vec<_>, Vec<_>) = inner_assets
            .into_iter()
            .filter(|asset| asset.asset_type.is_some_and(is_reuploadable))
            .partition(|asset| asset.asset_type == Some(AssetType::Model));

        // Upload the normal assets concurrently, the same way as the assets in the place
        let inner_ctx = UploadContext {
            total_assets: inner_assets.len(),
            completed: Arc::new(AtomicUsize::new(0)),
            ..self.clone()
        };
        let tasks = inner_ctx.spawn_upload_tasks(inner_assets);
        if let Ok(inner_mapping) = collect_upload_results(tasks).await {
            asset_mapping.extend(inner_mapping);
        }

        // Models inside this model have to be uploaded before this one, so their new ids can be used
        for inner_model in inner_models {
            if depth + 1 >= MAX_MODEL_DEPTH {
                warn!(
                    "Model {} is nested too deep, skipping {:?}",
                    model_id, inner_model.request_id
                );
                continue;
            }

            let inner_id = inner_model.request_id.clone();
            if let Err(e) = self
                .reupload_model(inner_model, asset_mapping, visited, depth + 1)
                .await
            {
                error!("Failed to reupload model {:?}: {:?}", inner_id, e);
            }
        }

        parser.adjust_special_mesh_offsets(asset_mapping, &self.uploader.mesh_centers());
        parser.update_script_assets(asset_mapping);
        parser.update_game_assets(asset_mapping);

        // Always uploaded as binary, even if the original model was xml
        let mut new_model_data = Vec::new();
        match write_dom(&mut new_model_data, &parser.dom, false) {
            Ok(()) => Bytes::from(new_model_data),
            Err(e) => {
                warn!(
                    "Couldn't save model {} with the new ids, uploading the original: {}",
                    model_id, e
                );
                model_data
            }
        }
    }
}
