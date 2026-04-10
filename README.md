# lancelot

A CLI tool for inspecting Lance files and datasets. Supports local paths and remote URIs (s3://, gs://, az://).

## Installation

```bash
make install
```

This builds a release binary and installs it to `~/.local/bin/lancelot`.

## Usage

### Global flags

| Flag | Values | Default | Description |
|------|--------|---------|-------------|
| `-o`, `--output` | `pretty`, `json` | `pretty` | Output format |

---

### `lancelot file` — inspect a Lance file

```
lancelot file [OPTIONS] [PATH]
```

Detects the file type from the extension:
- `.lance` — data file (schema, row count, column stats; or raw record batches)
- `.manifest` — manifest file (storage stats, schema, config; or full fragment listing)

**Options**

| Flag | Values | Default | Description |
|------|--------|---------|-------------|
| `-m`, `--mode` | `summary`, `raw` | `summary` | Display mode |

**Examples**

```bash
# Data file summary (pretty)
lancelot file path/to/data.lance

# Data file — all rows as JSON
lancelot file -o json -m raw path/to/data.lance

# Manifest summary
lancelot file path/to/_versions/5.manifest

# Manifest raw fragment listing
lancelot file -m raw path/to/_versions/5.manifest

# Read path from stdin
echo "s3://my-bucket/table/_versions/3.manifest" | lancelot file

# Remote data file
lancelot file s3://my-bucket/table/data/0001.lance
```

---

### `lancelot table` — inspect a Lance dataset directory

```
lancelot table [OPTIONS] [PATH]
```

Opens the latest version of the dataset and prints:
- URI, table version, format version, timestamp, writer
- Storage stats (fragments, rows, deletions, size)
- Schema
- Config and metadata key-value pairs
- Secondary indices (name, fields, UUID, dataset version, covered fragments)
- Recent version history (last 10) with manifest paths

**Examples**

```bash
# Pretty summary
lancelot table path/to/my_table

# JSON output
lancelot table -o json path/to/my_table

# Remote table
lancelot table s3://my-bucket/my_table

# Read path from stdin
echo "gs://my-bucket/my_table" | lancelot table
```

---

### Path input

All subcommands accept the path as:
- A positional argument: `lancelot table my_table/`
- Stdin: `echo my_table/ | lancelot table` (or pass `-` explicitly)
