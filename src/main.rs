mod output;

use std::io::{self, BufRead};
use std::sync::Arc;

use arrow::util::pretty::print_batches;
use clap::{Parser, Subcommand, ValueEnum};
use futures::TryStreamExt;
use lance::dataset::Dataset;
use lance_index::traits::DatasetIndexExt;
use lance_core::cache::LanceCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::reader::{FileReader, FileReaderOptions};
use lance_io::{
    object_store::ObjectStore,
    scheduler::{ScanScheduler, SchedulerConfig},
    utils::CachedFileSize,
    ReadBatchParams,
};
use lance_table::format::IndexMetadata;
use lance_table::format::Manifest;
use lance_table::io::commit::{ManifestNamingScheme, VERSIONS_DIR};
use lance_table::io::manifest::read_manifest;

use output::{
    ColumnStatInfo, DataSummary, FieldInfo, FileSizeInfo, FragmentDetail, DataFileDetail,
    IndexInfo, ManifestRaw, ManifestSummary, OutputFormat, StorageInfo, TableSummary, VersionInfo,
    render_data_summary, render_manifest_raw, render_manifest_summary, render_table_summary,
};

// ---------------------------------------------------------------------------
// Path resolution
// ---------------------------------------------------------------------------

fn read_path_from_stdin() -> anyhow::Result<String> {
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        let trimmed = line.trim().to_string();
        if !trimmed.is_empty() {
            return Ok(trimmed);
        }
    }
    anyhow::bail!("no path provided on stdin");
}

fn resolve_path(arg: Option<String>) -> anyhow::Result<String> {
    match arg.as_deref() {
        None | Some("-") => read_path_from_stdin(),
        Some(p) => Ok(p.to_string()),
    }
}

/// Build the absolute manifest path for a given version.
fn manifest_uri(base_uri: &str, scheme: ManifestNamingScheme, version: u64) -> String {
    let base = base_uri.trim_end_matches('/');
    let filename = match scheme {
        ManifestNamingScheme::V1 => format!("{version}.manifest"),
        ManifestNamingScheme::V2 => {
            let inverted = u64::MAX - version;
            format!("{inverted:020}.manifest")
        }
    };
    format!("{base}/{VERSIONS_DIR}/{filename}")
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "lancelot", about = "CLI for inspecting Lance files and datasets")]
struct Cli {
    /// Output format
    #[arg(short = 'o', long = "output", global = true, default_value = "pretty")]
    output: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Inspect a Lance file (.lance or .manifest)
    File {
        /// Path to the Lance file — local path, remote URI (s3://, gs://, az://…),
        /// or omit / pass - to read the path from stdin.
        /// Can be given positionally or via --path.
        #[arg(value_name = "PATH", index = 1)]
        path_pos: Option<String>,

        #[arg(long = "path", value_name = "PATH", hide = true, conflicts_with = "path_pos")]
        path: Option<String>,

        /// Display mode
        #[arg(short = 'm', long = "mode", default_value = "summary")]
        mode: FileMode,
    },

    /// Inspect a Lance table (dataset directory)
    Table {
        /// Path to the Lance table directory — local path, remote URI (s3://, gs://, az://…),
        /// or omit / pass - to read the path from stdin.
        /// Can be given positionally or via --path.
        #[arg(value_name = "PATH", index = 1)]
        path_pos: Option<String>,

        #[arg(long = "path", value_name = "PATH", hide = true, conflicts_with = "path_pos")]
        path: Option<String>,
    },
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum FileMode {
    /// Print file metadata summary (schema, row count, column statistics)
    Summary,
    /// Print full detail (raw record batches for data files, fragment list for manifests)
    Raw,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let fmt = cli.output;
    match cli.command {
        Commands::File { path_pos, path, mode } => {
            let uri = resolve_path(path_pos.or(path))?;
            cmd_file(&uri, mode, fmt).await?;
        }
        Commands::Table { path_pos, path } => {
            let uri = resolve_path(path_pos.or(path))?;
            cmd_table(&uri, fmt).await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// File type detection
// ---------------------------------------------------------------------------

enum FileType {
    Data,
    Manifest,
}

/// Detect file type from the URI suffix, stripping query params first.
fn detect_file_type(uri: &str) -> anyhow::Result<FileType> {
    let base = uri.split('?').next().unwrap_or(uri).trim_end_matches('/');
    if base.ends_with(".lance") {
        Ok(FileType::Data)
    } else if base.ends_with(".manifest") {
        Ok(FileType::Manifest)
    } else {
        let ext = base.rsplit('.').next().unwrap_or("(none)");
        anyhow::bail!(
            "unrecognised file extension '.{ext}' — supported: .lance (data file), .manifest"
        )
    }
}

// ---------------------------------------------------------------------------
// lancelot file
// ---------------------------------------------------------------------------

async fn cmd_file(uri: &str, mode: FileMode, fmt: OutputFormat) -> anyhow::Result<()> {
    match detect_file_type(uri)? {
        FileType::Data => cmd_data_file(uri, mode, fmt).await,
        FileType::Manifest => cmd_manifest_file(uri, mode, fmt).await,
    }
}

async fn cmd_data_file(uri: &str, mode: FileMode, fmt: OutputFormat) -> anyhow::Result<()> {
    let (object_store, path) = ObjectStore::from_uri(uri).await?;
    let scheduler = ScanScheduler::new(
        object_store.clone(),
        SchedulerConfig::max_bandwidth(&object_store),
    );
    let file_scheduler = scheduler
        .open_file(&path, &CachedFileSize::unknown())
        .await?;

    let cache = Arc::new(LanceCache::with_capacity(128 * 1024 * 1024));
    let reader = FileReader::try_open(
        file_scheduler,
        None,
        Arc::new(DecoderPlugins::default()),
        &cache,
        FileReaderOptions::default(),
    )
    .await?;

    match mode {
        FileMode::Summary => {
            let summary = collect_data_summary(&reader);
            render_data_summary(&summary, fmt);
        }
        FileMode::Raw => print_file_raw(reader, fmt).await?,
    }

    Ok(())
}

async fn cmd_manifest_file(uri: &str, mode: FileMode, fmt: OutputFormat) -> anyhow::Result<()> {
    let (object_store, path) = ObjectStore::from_uri(uri).await?;
    let manifest = read_manifest(&object_store, &path, None).await?;
    match mode {
        FileMode::Summary => {
            let summary = collect_manifest_summary(&manifest);
            render_manifest_summary(&summary, fmt);
        }
        FileMode::Raw => {
            let raw = collect_manifest_raw(&manifest);
            render_manifest_raw(&raw, fmt);
        }
    }
    Ok(())
}

fn collect_manifest_summary(manifest: &Manifest) -> ManifestSummary {
    let summary = manifest.summary();
    let format_version = manifest
        .data_storage_format
        .lance_file_version()
        .map(|v| v.resolve().to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    ManifestSummary {
        table_version: manifest.version,
        format_version,
        timestamp: manifest.timestamp().to_rfc3339(),
        tag: manifest.tag.clone(),
        written_by: manifest.writer_version.as_ref().map(|w| w.library.clone()),
        storage: StorageInfo {
            fragments: summary.total_fragments,
            data_files: summary.total_data_files,
            total_rows: summary.total_rows,
            deleted_rows: summary.total_deletion_file_rows,
            deletion_files: summary.total_deletion_files,
            files_size_bytes: summary.total_files_size,
        },
        schema: manifest
            .schema
            .fields
            .iter()
            .map(|f| FieldInfo {
                name: f.name.clone(),
                data_type: format!("{:?}", f.data_type()),
            })
            .collect(),
        config: manifest.config.clone(),
        metadata: manifest.table_metadata.clone(),
    }
}

fn collect_manifest_raw(manifest: &Manifest) -> ManifestRaw {
    let format_version = manifest
        .data_storage_format
        .lance_file_version()
        .map(|v| v.resolve().to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    let fragments = manifest
        .fragments
        .iter()
        .map(|frag| FragmentDetail {
            id: frag.id,
            num_rows: frag.physical_rows.map(|n| n as u64),
            data_files: frag
                .files
                .iter()
                .map(|df| DataFileDetail {
                    path: df.path.clone(),
                    field_ids: df.fields.clone(),
                })
                .collect(),
            deletion_file: frag.deletion_file.as_ref().map(|d| {
                format!(
                    "_deletions/{}-{}.{}",
                    d.read_version,
                    d.id,
                    d.file_type.suffix()
                )
            }),
        })
        .collect();

    ManifestRaw {
        table_version: manifest.version,
        format_version,
        timestamp: manifest.timestamp().to_rfc3339(),
        fragments,
    }
}

fn collect_data_summary(reader: &FileReader) -> DataSummary {
    let meta = reader.metadata();
    let schema = reader.schema();
    let stats = reader.file_statistics();

    let total_bytes = meta.num_data_bytes
        + meta.num_column_metadata_bytes
        + meta.num_global_buffer_bytes
        + meta.num_footer_bytes;

    DataSummary {
        format_version: format!("{}.{}", meta.major_version, meta.minor_version),
        rows: meta.num_rows,
        size: FileSizeInfo {
            total_bytes,
            data_bytes: meta.num_data_bytes,
            column_metadata_bytes: meta.num_column_metadata_bytes,
            global_buffer_bytes: meta.num_global_buffer_bytes,
            footer_bytes: meta.num_footer_bytes,
        },
        schema: schema
            .fields
            .iter()
            .map(|f| FieldInfo {
                name: f.name.clone(),
                data_type: format!("{:?}", f.data_type()),
            })
            .collect(),
        column_statistics: schema
            .fields
            .iter()
            .zip(stats.columns.iter())
            .map(|(f, c)| ColumnStatInfo {
                column: f.name.clone(),
                pages: c.num_pages,
                bytes: c.size_bytes,
            })
            .collect(),
    }
}

async fn print_file_raw(reader: FileReader, fmt: OutputFormat) -> anyhow::Result<()> {
    let batch_stream =
        reader.read_stream(ReadBatchParams::RangeFull, 4096, 16, FilterExpression::no_filter())?;

    let batches: Vec<_> = batch_stream.try_collect().await?;
    let arrow_batches: Vec<arrow::array::RecordBatch> =
        batches.into_iter().map(arrow::array::RecordBatch::from).collect();

    match fmt {
        OutputFormat::Pretty => print_batches(&arrow_batches)?,
        OutputFormat::Json => {
            let mut buf = Vec::new();
            let mut writer = arrow::json::ArrayWriter::new(&mut buf);
            for batch in &arrow_batches {
                writer.write(batch)?;
            }
            writer.finish()?;
            println!("{}", String::from_utf8(buf)?);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// lancelot table
// ---------------------------------------------------------------------------

async fn cmd_table(uri: &str, fmt: OutputFormat) -> anyhow::Result<()> {
    let dataset = Dataset::open(uri).await?;
    let summary = collect_table_summary(&dataset).await?;
    render_table_summary(&summary, fmt);
    Ok(())
}

async fn collect_table_summary(dataset: &Dataset) -> anyhow::Result<TableSummary> {
    let manifest = dataset.manifest();
    let summary = manifest.summary();
    let schema = dataset.schema();
    let naming_scheme = dataset.manifest_location().naming_scheme;
    let base_uri = dataset.uri();

    let indices = dataset.load_indices().await?;
    let index_infos = collect_indices(dataset, &indices);

    let versions = dataset.versions().await?;
    let total = versions.len();
    let show = total.min(10);
    let version_infos: Vec<VersionInfo> = versions
        .iter()
        .rev()
        .take(show)
        .map(|v| VersionInfo {
            version: v.version,
            timestamp: v.timestamp.to_rfc3339(),
            tag: v.metadata.get("tag").cloned().unwrap_or_default(),
            manifest: manifest_uri(base_uri, naming_scheme, v.version),
        })
        .collect();

    let format_version = manifest
        .data_storage_format
        .lance_file_version()
        .map(|v| v.resolve().to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    Ok(TableSummary {
        uri: dataset.uri().to_string(),
        table_version: manifest.version,
        format_version,
        tag: manifest.tag.clone(),
        timestamp: manifest.timestamp().to_rfc3339(),
        written_by: manifest.writer_version.as_ref().map(|w| w.library.clone()),
        storage: StorageInfo {
            fragments: summary.total_fragments,
            data_files: summary.total_data_files,
            total_rows: summary.total_rows,
            deleted_rows: summary.total_deletion_file_rows,
            deletion_files: summary.total_deletion_files,
            files_size_bytes: summary.total_files_size,
        },
        schema: schema
            .fields
            .iter()
            .map(|f| FieldInfo {
                name: f.name.clone(),
                data_type: format!("{:?}", f.data_type()),
            })
            .collect(),
        config: dataset.config().clone(),
        metadata: dataset.metadata().clone(),
        indices: index_infos,
        versions: version_infos,
    })
}

fn collect_indices(dataset: &Dataset, indices: &[IndexMetadata]) -> Vec<IndexInfo> {
    let schema = dataset.schema();
    indices
        .iter()
        .filter(|idx| !lance_index::is_system_index(idx))
        .map(|idx| {
            let fields: Vec<String> = idx
                .fields
                .iter()
                .filter_map(|fid| schema.field_by_id(*fid).map(|f| f.name.clone()))
                .collect();
            let fields = if fields.is_empty() {
                idx.fields.iter().map(|id| format!("#{id}")).collect()
            } else {
                fields
            };

            IndexInfo {
                name: idx.name.clone(),
                fields,
                uuid: idx.uuid.to_string(),
                dataset_version: idx.dataset_version,
                created_at: idx.created_at.map(|t| t.to_rfc3339()),
                covered_fragments: idx.fragment_bitmap.as_ref().map(|b| b.len()),
            }
        })
        .collect()
}
