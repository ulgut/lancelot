#!/usr/bin/env python3
"""
generate.py — create sample Lance tables and raw Lance files for testing lancelot.

Usage:
    python generate.py [--out-dir OUT_DIR]

Outputs (written to OUT_DIR, default: ./fixtures):
    tables/
        simple/          — basic int+string+float table, 3 versions, 1 scalar index
        vectors/         — embedding table with a vector index
        nested/          — table with nested structs and lists
        with_deletions/  — table that has deleted rows and a deletion file

    files/
        simple.lance     — raw Lance file (no dataset wrapper)
        wide.lance       — many columns of different types
"""

import argparse
import shutil
from pathlib import Path

import numpy as np
import pyarrow as pa
import lance
from lance.file import LanceFileWriter


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def rm(path: Path):
    if path.exists():
        shutil.rmtree(path)


def write_table(uri: str, batches: list[pa.RecordBatch], mode: str = "create", **kwargs) -> lance.LanceDataset:
    table = pa.Table.from_batches(batches)
    return lance.write_dataset(table, uri, mode=mode, **kwargs)


# ---------------------------------------------------------------------------
# tables
# ---------------------------------------------------------------------------

def make_simple(out: Path):
    """int64 id, utf8 name, float64 score — 3 versions, btree index on id."""
    uri = str(out / "simple")
    rm(Path(uri))

    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.utf8()),
        pa.field("score", pa.float64()),
    ])

    # version 1 — initial write
    batch1 = pa.record_batch({
        "id":    pa.array(list(range(100)), type=pa.int64()),
        "name":  pa.array([f"user_{i}" for i in range(100)]),
        "score": pa.array([float(i) * 0.5 for i in range(100)]),
    }, schema=schema)
    ds = write_table(uri, [batch1])

    # version 2 — append
    batch2 = pa.record_batch({
        "id":    pa.array(list(range(100, 200)), type=pa.int64()),
        "name":  pa.array([f"user_{i}" for i in range(100, 200)]),
        "score": pa.array([float(i) * 0.5 for i in range(100, 200)]),
    }, schema=schema)
    ds = write_table(uri, [batch2], mode="append")

    # version 3 — another append
    batch3 = pa.record_batch({
        "id":    pa.array(list(range(200, 250)), type=pa.int64()),
        "name":  pa.array([f"user_{i}" for i in range(200, 250)]),
        "score": pa.array([float(i) * 0.5 for i in range(200, 250)]),
    }, schema=schema)
    ds = write_table(uri, [batch3], mode="append")

    # scalar btree index on id
    ds.create_scalar_index("id", index_type="BTREE")

    print(f"  [simple]   {ds.count_rows()} rows, {ds.version} versions at {uri}")


def make_vectors(out: Path):
    """id + 128-dim float32 embedding — IVF_PQ vector index."""
    uri = str(out / "vectors")
    rm(Path(uri))

    dim = 128
    n = 1000
    rng = np.random.default_rng(42)

    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("embedding", pa.list_(pa.float32(), dim)),
        pa.field("label", pa.utf8()),
    ])

    vecs = rng.random((n, dim), dtype=np.float32)
    batch = pa.record_batch({
        "id":        pa.array(list(range(n)), type=pa.int64()),
        "embedding": pa.FixedSizeListArray.from_arrays(
                         pa.array(vecs.flatten(), type=pa.float32()), dim),
        "label":     pa.array([f"class_{i % 10}" for i in range(n)]),
    }, schema=schema)

    ds = write_table(uri, [batch])

    ds.create_index(
        "embedding",
        index_type="IVF_PQ",
        num_partitions=16,
        num_sub_vectors=16,
        metric="cosine",
    )

    print(f"  [vectors]  {ds.count_rows()} rows, vector index at {uri}")


def make_nested(out: Path):
    """Nested structs and variable-length lists."""
    uri = str(out / "nested")
    rm(Path(uri))

    n = 200
    address_type = pa.struct([
        pa.field("street", pa.utf8()),
        pa.field("city", pa.utf8()),
        pa.field("zip", pa.utf8()),
    ])
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("address", address_type),
        pa.field("tags", pa.list_(pa.utf8())),
        pa.field("scores", pa.list_(pa.float32())),
        pa.field("active", pa.bool_()),
    ])

    rng = np.random.default_rng(0)
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]

    addresses = pa.array(
        [{"street": f"{i} Main St", "city": cities[i % 5], "zip": f"{10000 + i:05d}"}
         for i in range(n)],
        type=address_type,
    )
    tags = pa.array(
        [["tag_a", "tag_b"] if i % 3 == 0 else ["tag_c"] for i in range(n)],
        type=pa.list_(pa.utf8()),
    )
    scores_flat = rng.random(n * 4, dtype=np.float32)
    scores = pa.array(
        [scores_flat[i * 4: (i + 1) * 4].tolist() for i in range(n)],
        type=pa.list_(pa.float32()),
    )

    batch = pa.record_batch({
        "id":      pa.array(list(range(n)), type=pa.int64()),
        "address": addresses,
        "tags":    tags,
        "scores":  scores,
        "active":  pa.array([bool(i % 2) for i in range(n)]),
    }, schema=schema)

    ds = write_table(uri, [batch])
    print(f"  [nested]   {ds.count_rows()} rows at {uri}")


def make_with_deletions(out: Path):
    """Table with ~20% rows deleted so there are deletion files."""
    uri = str(out / "with_deletions")
    rm(Path(uri))

    n = 500
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("value", pa.float64()),
    ])
    batch = pa.record_batch({
        "id":    pa.array(list(range(n)), type=pa.int64()),
        "value": pa.array([float(i) for i in range(n)]),
    }, schema=schema)

    ds = write_table(uri, [batch])
    ds.delete("id % 5 = 0")   # delete every 5th row (~20%)

    print(f"  [deletions] {ds.count_rows()} rows remaining ({n - ds.count_rows()} deleted) at {uri}")


# ---------------------------------------------------------------------------
# raw files
# ---------------------------------------------------------------------------

def make_simple_file(out: Path):
    """Raw Lance file with a handful of columns."""
    path = str(out / "simple.lance")

    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.utf8()),
        pa.field("value", pa.float32()),
        pa.field("flag", pa.bool_()),
    ])
    n = 300
    batch = pa.record_batch({
        "id":    pa.array(list(range(n)), type=pa.int64()),
        "name":  pa.array([f"item_{i}" for i in range(n)]),
        "value": pa.array([float(i) * 1.1 for i in range(n)], type=pa.float32()),
        "flag":  pa.array([bool(i % 2) for i in range(n)]),
    }, schema=schema)

    with LanceFileWriter(path, schema) as writer:
        writer.write_batch(batch)

    print(f"  [simple.lance]  {n} rows at {path}")


def make_wide_file(out: Path):
    """Raw Lance file with many columns of different types."""
    path = str(out / "wide.lance")

    rng = np.random.default_rng(7)
    n = 100

    fields = [
        pa.field("id", pa.int32()),
        pa.field("int8_col", pa.int8()),
        pa.field("int16_col", pa.int16()),
        pa.field("int64_col", pa.int64()),
        pa.field("uint32_col", pa.uint32()),
        pa.field("float32_col", pa.float32()),
        pa.field("float64_col", pa.float64()),
        pa.field("bool_col", pa.bool_()),
        pa.field("str_col", pa.utf8()),
        pa.field("binary_col", pa.binary()),
        pa.field("ts_col", pa.timestamp("ms")),
        pa.field("date_col", pa.date32()),
        pa.field("vec_col", pa.list_(pa.float32(), 8)),
    ]
    schema = pa.schema(fields)

    arrays = {
        "id":          pa.array(list(range(n)), type=pa.int32()),
        "int8_col":    pa.array(rng.integers(-128, 127, n, dtype=np.int8)),
        "int16_col":   pa.array(rng.integers(-32768, 32767, n, dtype=np.int16)),
        "int64_col":   pa.array(rng.integers(-(2**62), 2**62, n, dtype=np.int64)),
        "uint32_col":  pa.array(rng.integers(0, 2**32 - 1, n, dtype=np.uint32)),
        "float32_col": pa.array(rng.random(n, dtype=np.float32)),
        "float64_col": pa.array(rng.random(n, dtype=np.float64)),
        "bool_col":    pa.array(rng.integers(0, 2, n, dtype=bool).tolist()),
        "str_col":     pa.array([f"val_{i:04d}" for i in range(n)]),
        "binary_col":  pa.array([bytes(rng.integers(0, 256, 8, dtype=np.uint8)) for _ in range(n)]),
        "ts_col":      pa.array(list(range(1_700_000_000_000, 1_700_000_000_000 + n * 1000, 1000)),
                                type=pa.timestamp("ms")),
        "date_col":    pa.array(list(range(19000, 19000 + n)), type=pa.date32()),
        "vec_col":     pa.FixedSizeListArray.from_arrays(
                           pa.array(rng.random(n * 8, dtype=np.float32), type=pa.float32()), 8),
    }
    batch = pa.record_batch(arrays, schema=schema)

    with LanceFileWriter(path, schema) as writer:
        writer.write_batch(batch)

    print(f"  [wide.lance]    {n} rows, {len(fields)} columns at {path}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate Lance test fixtures")
    parser.add_argument("--out-dir", default="fixtures", help="Output directory (default: fixtures)")
    args = parser.parse_args()

    out = Path(args.out_dir)
    tables_dir = out / "tables"
    files_dir = out / "files"
    tables_dir.mkdir(parents=True, exist_ok=True)
    files_dir.mkdir(parents=True, exist_ok=True)

    print("Generating tables...")
    make_simple(tables_dir)
    make_vectors(tables_dir)
    make_nested(tables_dir)
    make_with_deletions(tables_dir)

    print("\nGenerating raw files...")
    make_simple_file(files_dir)
    make_wide_file(files_dir)

    print(f"\nDone. Fixtures written to: {out.resolve()}")


if __name__ == "__main__":
    main()
