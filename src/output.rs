use std::collections::HashMap;

use serde::Serialize;

// ---------------------------------------------------------------------------
// Output format
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    /// Human-readable text
    Pretty,
    /// Machine-readable JSON
    Json,
}

// ---------------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct FieldInfo {
    pub name: String,
    pub data_type: String,
}

#[derive(Serialize)]
pub struct StorageInfo {
    pub fragments: u64,
    pub data_files: u64,
    pub total_rows: u64,
    pub deleted_rows: u64,
    pub deletion_files: u64,
    pub files_size_bytes: u64,
}

// ---------------------------------------------------------------------------
// Data file (.lance)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct DataSummary {
    /// Lance file format version (e.g. "2.1")
    pub format_version: String,
    pub rows: u64,
    pub size: FileSizeInfo,
    pub schema: Vec<FieldInfo>,
    pub column_statistics: Vec<ColumnStatInfo>,
}

#[derive(Serialize)]
pub struct FileSizeInfo {
    pub total_bytes: u64,
    pub data_bytes: u64,
    pub column_metadata_bytes: u64,
    pub global_buffer_bytes: u64,
    pub footer_bytes: u64,
}

#[derive(Serialize)]
pub struct ColumnStatInfo {
    pub column: String,
    pub pages: usize,
    pub bytes: u64,
}

// ---------------------------------------------------------------------------
// Manifest file (.manifest)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct ManifestSummary {
    pub table_version: u64,
    pub format_version: String,
    pub timestamp: String,
    pub tag: Option<String>,
    pub written_by: Option<String>,
    pub storage: StorageInfo,
    pub schema: Vec<FieldInfo>,
    pub config: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
}

/// Full fragment listing — raw mode for manifest files.
#[derive(Serialize)]
pub struct ManifestRaw {
    pub table_version: u64,
    pub format_version: String,
    pub timestamp: String,
    pub fragments: Vec<FragmentDetail>,
}

#[derive(Serialize)]
pub struct FragmentDetail {
    pub id: u64,
    pub num_rows: Option<u64>,
    pub data_files: Vec<DataFileDetail>,
    pub deletion_file: Option<String>,
}

#[derive(Serialize)]
pub struct DataFileDetail {
    pub path: String,
    pub field_ids: Vec<i32>,
}

// ---------------------------------------------------------------------------
// Table summary
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct TableSummary {
    pub uri: String,
    /// Monotonically increasing commit counter (latest version opened)
    pub table_version: u64,
    /// Lance file format version used to write the data files (e.g. "2.1")
    pub format_version: String,
    pub tag: Option<String>,
    pub timestamp: String,
    pub written_by: Option<String>,
    pub storage: StorageInfo,
    pub schema: Vec<FieldInfo>,
    pub config: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
    pub indices: Vec<IndexInfo>,
    pub versions: Vec<VersionInfo>,
}

#[derive(Serialize)]
pub struct IndexInfo {
    pub name: String,
    pub fields: Vec<String>,
    pub uuid: String,
    pub dataset_version: u64,
    pub created_at: Option<String>,
    pub covered_fragments: Option<u64>,
}

#[derive(Serialize)]
pub struct VersionInfo {
    pub version: u64,
    pub timestamp: String,
    pub tag: String,
    pub manifest: String,
}

// ---------------------------------------------------------------------------
// Rendering — data file
// ---------------------------------------------------------------------------

pub fn render_data_summary(s: &DataSummary, fmt: OutputFormat) {
    match fmt {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(s).unwrap()),
        OutputFormat::Pretty => {
            println!("=== Lance Data File Summary ===");
            println!("Format version:  {}", s.format_version);
            println!("Rows:            {}", s.rows);
            println!("Size:            {} bytes total", s.size.total_bytes);
            println!(
                "                 {} data  |  {} col-meta  |  {} global-bufs  |  {} footer",
                s.size.data_bytes,
                s.size.column_metadata_bytes,
                s.size.global_buffer_bytes,
                s.size.footer_bytes,
            );

            println!("\n=== Schema ===");
            for f in &s.schema {
                println!("  {:30} {}", f.name, f.data_type);
            }

            if !s.column_statistics.is_empty() {
                println!("\n=== Column Statistics ===");
                println!("{:<30} {:>10}  {:>12}", "Column", "Pages", "Bytes");
                println!("{}", "-".repeat(56));
                for c in &s.column_statistics {
                    println!("{:<30} {:>10}  {:>12}", c.column, c.pages, c.bytes);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering — manifest file
// ---------------------------------------------------------------------------

pub fn render_manifest_summary(s: &ManifestSummary, fmt: OutputFormat) {
    match fmt {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(s).unwrap()),
        OutputFormat::Pretty => {
            println!("=== Lance Manifest ===");
            println!("Table version:     {}", s.table_version);
            println!("Format version:    {}", s.format_version);
            if let Some(tag) = &s.tag {
                println!("Tag:               {}", tag);
            }
            println!("Timestamp:         {}", s.timestamp);
            if let Some(w) = &s.written_by {
                println!("Written by:        {}", w);
            }

            println!("\n=== Storage ===");
            println!("Fragments:         {}", s.storage.fragments);
            println!("Data files:        {}", s.storage.data_files);
            println!("Total rows:        {}", s.storage.total_rows);
            println!("Deleted rows:      {}", s.storage.deleted_rows);
            println!("Deletion files:    {}", s.storage.deletion_files);
            if s.storage.files_size_bytes > 0 {
                println!("Files size:        {} bytes", s.storage.files_size_bytes);
            }

            println!("\n=== Schema ===");
            for f in &s.schema {
                println!("  {:30} {}", f.name, f.data_type);
            }

            if !s.config.is_empty() {
                println!("\n=== Config ===");
                let mut keys: Vec<_> = s.config.keys().collect();
                keys.sort();
                for k in keys {
                    println!("  {} = {}", k, s.config[k]);
                }
            }

            if !s.metadata.is_empty() {
                println!("\n=== Metadata ===");
                let mut keys: Vec<_> = s.metadata.keys().collect();
                keys.sort();
                for k in keys {
                    println!("  {} = {}", k, s.metadata[k]);
                }
            }
        }
    }
}

pub fn render_manifest_raw(s: &ManifestRaw, fmt: OutputFormat) {
    match fmt {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(s).unwrap()),
        OutputFormat::Pretty => {
            println!("=== Lance Manifest (raw) ===");
            println!("Table version:   {}", s.table_version);
            println!("Format version:  {}", s.format_version);
            println!("Timestamp:       {}", s.timestamp);
            println!("\n{} fragments:\n", s.fragments.len());
            for frag in &s.fragments {
                println!(
                    "Fragment #{} — {} rows{}",
                    frag.id,
                    frag.num_rows.map_or("?".to_string(), |n| n.to_string()),
                    frag.deletion_file
                        .as_deref()
                        .map_or(String::new(), |d| format!("  (deletion: {d})")),
                );
                for df in &frag.data_files {
                    println!(
                        "  data: {}  [fields: {:?}]",
                        df.path, df.field_ids
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering — table
// ---------------------------------------------------------------------------

pub fn render_table_summary(s: &TableSummary, fmt: OutputFormat) {
    match fmt {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(s).unwrap()),
        OutputFormat::Pretty => {
            println!("=== Lance Table Summary ===");
            println!("URI:               {}", s.uri);
            println!("Table version:     {}", s.table_version);
            println!("Format version:    {}", s.format_version);
            if let Some(tag) = &s.tag {
                println!("Tag:               {}", tag);
            }
            println!("Timestamp:         {}", s.timestamp);
            if let Some(w) = &s.written_by {
                println!("Written by:        {}", w);
            }

            println!("\n=== Storage ===");
            println!("Fragments:         {}", s.storage.fragments);
            println!("Data files:        {}", s.storage.data_files);
            println!("Total rows:        {}", s.storage.total_rows);
            println!("Deleted rows:      {}", s.storage.deleted_rows);
            println!("Deletion files:    {}", s.storage.deletion_files);
            if s.storage.files_size_bytes > 0 {
                println!("Files size:        {} bytes", s.storage.files_size_bytes);
            }

            println!("\n=== Schema ===");
            for f in &s.schema {
                println!("  {:30} {}", f.name, f.data_type);
            }

            if !s.config.is_empty() {
                println!("\n=== Config ===");
                let mut keys: Vec<_> = s.config.keys().collect();
                keys.sort();
                for k in keys {
                    println!("  {} = {}", k, s.config[k]);
                }
            }

            if !s.metadata.is_empty() {
                println!("\n=== Metadata ===");
                let mut keys: Vec<_> = s.metadata.keys().collect();
                keys.sort();
                for k in keys {
                    println!("  {} = {}", k, s.metadata[k]);
                }
            }

            println!("\n=== Indices ===");
            if s.indices.is_empty() {
                println!("  (none)");
            } else {
                for idx in &s.indices {
                    println!("  Name:          {}", idx.name);
                    println!("  Fields:        {}", idx.fields.join(", "));
                    println!("  UUID:          {}", idx.uuid);
                    println!("  Dataset ver:   {}", idx.dataset_version);
                    if let Some(ts) = &idx.created_at {
                        println!("  Created at:    {}", ts);
                    }
                    if let Some(n) = idx.covered_fragments {
                        println!("  Covers frags:  {} fragments", n);
                    }
                    println!();
                }
            }

            let total = s.versions.len();
            let show = total.min(10);
            println!("\n=== Recent Versions (last {show} of {total}) ===");
            println!(
                "{:>10}  {:>35}  {:<6}  {}",
                "Version", "Timestamp", "Tag", "Manifest"
            );
            println!("{}", "-".repeat(110));
            for v in s.versions.iter().take(show) {
                println!(
                    "{:>10}  {:>35}  {:<6}  {}",
                    v.version, v.timestamp, v.tag, v.manifest,
                );
            }
        }
    }
}
