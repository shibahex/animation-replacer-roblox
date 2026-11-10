use animation_replace_roblox::StudioParser;
use animation_replace_roblox::asset_manager::uploader::{AssetUploader, is_reuploadable};
use clap::{Parser, ValueEnum};
use roboat::catalog::AssetType;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info};
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

    /// Only find the assets and show what would be reuploaded, without uploading or saving anything
    #[arg(long)]
    dry_run: bool,

    /// Only reupload these asset types, comma separated (like --only animation,audio). Others keep their id
    #[arg(long, value_enum, value_delimiter = ',')]
    only: Vec<AssetKind>,

    /// Look up the original name on Roblox for assets that don't get a name from the game
    /// (like assets only used in scripts). Costs one extra request per asset
    #[arg(long)]
    fetch_names: bool,
}

/// The asset types that can be picked with --only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum AssetKind {
    Animation,
    Audio,
    /// Images and decals
    Image,
    Model,
    Video,
    Mesh,
}

impl AssetKind {
    fn matches(self, asset_type: AssetType) -> bool {
        match self {
            AssetKind::Animation => asset_type == AssetType::Animation,
            AssetKind::Audio => asset_type == AssetType::Audio,
            AssetKind::Image => matches!(asset_type, AssetType::Image | AssetType::Decal),
            AssetKind::Model => asset_type == AssetType::Model,
            AssetKind::Video => asset_type == AssetType::Video,
            AssetKind::Mesh => asset_type == AssetType::Mesh,
        }
    }
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

#[tokio::main]
async fn main() -> ExitCode {
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
    let started = Instant::now();

    // Build the parser with the roboat client
    let builder = StudioParser::builder()
        .file_path(&file_path)
        .api_key(&args.api_key)
        .roblosecurity(&args.cookie);

    let mut parser = match builder.build() {
        Ok(parser) => parser,
        Err(e) => {
            error!("Error loading file: {}", e);
            return ExitCode::FAILURE;
        }
    };

    // Every asset id in the place and its scripts, looked up in one go so ids used in
    // both places are only fetched once
    let mut asset_ids = parser.asset_ids_in_instances();
    asset_ids.extend(parser.asset_ids_in_scripts(args.unformatted_ids));
    asset_ids.sort();
    asset_ids.dedup();
    info!("Found {} asset ids in the game file", asset_ids.len());

    // One uploader for the whole run, so caches and connections are shared
    let uploader = Arc::new(AssetUploader::new(args.cookie, args.api_key));
    uploader.add_asset_names(parser.asset_names());

    let mut all_assets = match uploader.fetch_animation_assets(asset_ids).await {
        Ok(assets) => assets,
        Err(e) => {
            error!("Failed to fetch the assets: {:?}", e);
            return ExitCode::FAILURE;
        }
    };

    if !args.only.is_empty() {
        all_assets.retain(|asset| {
            asset
                .asset_type
                .is_some_and(|asset_type| args.only.iter().any(|kind| kind.matches(asset_type)))
        });
        info!("Only reuploading {:?}", args.only);
    }

    let mut type_counts: BTreeMap<String, usize> = BTreeMap::new();
    for asset in &all_assets {
        let type_name = match asset.asset_type {
            Some(asset_type) if is_reuploadable(asset_type) => asset_type.to_string(),
            Some(asset_type) => format!("{} (skipped, can't be reuploaded)", asset_type),
            None => "Unknown (skipped)".to_string(),
        };
        *type_counts.entry(type_name).or_default() += 1;
    }

    info!(
        "Total assets fetched from game {} (in {:.1?})",
        all_assets.len(),
        started.elapsed()
    );
    for (type_name, count) in &type_counts {
        info!("  {}: {}", type_name, count);
    }

    if args.fetch_names {
        let lookup_started = Instant::now();
        let found = uploader.fetch_missing_names(&all_assets).await;
        info!(
            "Found {} original names (in {:.1?})",
            found,
            lookup_started.elapsed()
        );
    }

    if args.dry_run {
        info!("Dry run, nothing was uploaded or saved");
        return ExitCode::SUCCESS;
    }

    let asset_mapping = match Arc::clone(&uploader)
        .reupload_all_assets(all_assets, args.group, args.threads)
        .await
    {
        Ok(asset_mapping) => asset_mapping,
        Err(e) => {
            error!("Failed to upload assets: {:?}", e);
            return ExitCode::FAILURE;
        }
    };

    parser.adjust_special_mesh_offsets(&asset_mapping, &uploader.mesh_centers());
    parser.update_script_assets(&asset_mapping);
    parser.update_game_assets(&asset_mapping);

    let output = args.output.unwrap_or(file_path);
    if let Err(e) = parser.save_to_rbxl(&output) {
        error!("Failed to save {}: {:?}", output, e);
        return ExitCode::FAILURE;
    }

    info!(
        "Saved {} with {} new asset ids (took {:.1?})",
        output,
        asset_mapping.len(),
        started.elapsed()
    );
    ExitCode::SUCCESS
}
