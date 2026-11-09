use animation_replace_roblox::StudioParser;
use animation_replace_roblox::asset_manager::uploader::AssetUploader;
use clap::Parser;
use roboat::assetdelivery::AssetBatchResponse;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Parser, Debug)]
struct Args {
    /// .ROBLOSECURITY cookie string [WARNING STRING REQUIRED]
    #[arg(long, short)]
    cookie: String,

    /// Open cloud API Key [with assets permissions]
    #[arg(long)]
    api_key: String,

    /// file PATH of the .rbxl file [REQUIRED]
    #[arg(long, short)]
    file: String,

    /// Save the copy instead replacing file [AVOID DATA LOSS]
    #[arg(long, short)]
    output: Option<String>,

    /// Required if the the game will be published to a Group [Id of the group]
    #[arg(long, short)]
    group: Option<u64>,

    /// How many concurrent tasks using semaphore. [defaulted to 5]
    #[arg(long, short)]
    threads: Option<u64>,

    /// Search every big digits in scripts if flag is active (instead of just scanning for rbxasset)
    #[arg(long, short)]
    unformatted_ids: bool,
}

fn cleanup_old_logs(log_dir: &str, prefix: &str, keep: usize) {
    let dir = Path::new(log_dir);

    if !dir.exists() {
        return;
    }

    let mut entries: Vec<_> = fs::read_dir(dir)
        .expect("Can't read log dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
        .collect();

    entries.sort_by_key(|e| {
        e.metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
    });

    if entries.len() > keep {
        for entry in &entries[..entries.len() - keep] {
            match fs::remove_file(entry.path()) {
                Ok(_) => println!("Deleted old log: {:?}", entry.path()),
                Err(e) => println!("Failed to delete {:?}: {}", entry.path(), e),
            }
        }
    }
}

/// Adds assets that haven't been seen yet, skipping duplicates.
fn collect_unique_assets(
    assets: Vec<AssetBatchResponse>,
    seen_ids: &mut HashSet<String>,
    all_assets: &mut Vec<AssetBatchResponse>,
) {
    for asset in assets {
        if let Some(asset_id) = asset.request_id.clone()
            && seen_ids.insert(asset_id)
        {
            all_assets.push(asset);
        }
    }
}

#[tokio::main]
async fn main() {
    // Clean up old logs, keeping only the 5 most recent
    cleanup_old_logs("logs", "app.log", 5);

    let file_appender = tracing_appender::rolling::hourly("logs", "app.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    // Log to both stdout and file
    tracing_subscriber::registry()
        // stdout: info and above
        .with(
            fmt::layer()
                .with_writer(std::io::stdout)
                .with_target(false) // removes `animation_replace_roblox::asset_manager::info`
                .with_thread_ids(false)
                .with_file(false)
                .with_line_number(false)
                .with_filter(EnvFilter::new("info")),
        )
        // file: debug and above
        .with(
            fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_filter(EnvFilter::new("debug")),
        )
        .init();

    let args = Args::parse();
    let file_path = shellexpand::tilde(&args.file).to_string();
    let mut seen_ids: HashSet<String> = HashSet::new();

    // Build the parser with the roboat client
    let builder = StudioParser::builder()
        .file_path(&file_path)
        .api_key(&args.api_key)
        .roblosecurity(&args.cookie);

    let mut parser = match builder.build() {
        Ok(parser) => parser,
        Err(e) => {
            warn!("Error loading file: {}", e);
            return;
        }
    };

    let mut all_assets: Vec<AssetBatchResponse> = Vec::new();

    match parser.workspace_assets().await {
        Ok(assets) => collect_unique_assets(assets, &mut seen_ids, &mut all_assets),
        Err(e) => warn!("Failed to fetch workspace assets: {:?}", e),
    }

    match parser.all_assets_in_scripts(args.unformatted_ids).await {
        Ok(assets) => collect_unique_assets(assets, &mut seen_ids, &mut all_assets),
        Err(e) => warn!("Failed to fetch script assets: {:?}", e),
    }

    let mut type_counts: BTreeMap<String, usize> = BTreeMap::new();
    for asset in &all_assets {
        let type_name = asset
            .asset_type
            .map(|asset_type| asset_type.to_string())
            .unwrap_or_else(|| "Unknown".to_string());
        *type_counts.entry(type_name).or_default() += 1;
    }

    info!("Total assets fetched from game {}", all_assets.len());
    for (type_name, count) in &type_counts {
        info!("  {}: {}", type_name, count);
    }

    let uploader = Arc::new(AssetUploader::new(args.cookie, args.api_key));
    match uploader
        .reupload_all_assets(all_assets, args.group, args.threads)
        .await
    {
        Ok(asset_mapping) => {
            parser.update_script_assets(&asset_mapping);
            parser.update_game_assets(&asset_mapping);
        }
        Err(e) => {
            warn!("Failed to upload assets: {:?}", e);
        }
    }

    if let Some(output) = args.output {
        parser.save_to_rbxl(output).unwrap();
    } else {
        parser.save_to_rbxl(file_path).unwrap();
    }
}
