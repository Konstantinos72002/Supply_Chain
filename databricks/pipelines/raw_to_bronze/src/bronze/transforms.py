"""
Pure PySpark transformations used by the Bronze ETL pipeline.

They work on batch AND streaming DataFrames, so they are unit-tested locally
(tests/) and reused unchanged in pipelines/bronze_pipeline.py.

Flow for every source:
    file source  ->  with_file_metadata()  ->  shape_csv() | shape_jsonl()
                 ->  add_audit_columns()   ->  Bronze table
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType

from bronze.config import (EXCLUDED_FILE_SUFFIXES, LOAD_STATUS_LOADED,
                           LOAD_STATUS_MALFORMED, LOAD_STATUS_SCHEMA_DRIFT,
                           SOURCES)

AUDIT_COLUMNS = ["source_file_name", "source_file_mod_ts", "ingestion_ts",
                 "batch_id", "source_system", "record_hash", "load_status"]


# --------------------------------------------------------------------------- #
# 1. File metadata (must be called directly on the file source)
# --------------------------------------------------------------------------- #
def with_file_metadata(df: DataFrame) -> DataFrame:
    """Capture the hidden `_metadata` column once and drop excluded files."""
    out = df.select("*", F.col("_metadata").alias("_file"))
    for suffix in EXCLUDED_FILE_SUFFIXES:
        out = out.where(~F.col("_file.file_name").endswith(suffix))
    return out


# --------------------------------------------------------------------------- #
# 2. Shaping (no business transformation - values stay strings)
# --------------------------------------------------------------------------- #
def shape_csv(df: DataFrame, source_name: str) -> DataFrame:
    """Contract columns + `_rescued_data` (filled by Auto Loader on Databricks)."""
    expected = SOURCES[source_name]["expected_columns"]
    rescued = (F.col("_rescued_data") if "_rescued_data" in df.columns
               else F.lit(None).cast("string"))
    return df.select(*[F.col(f"`{c}`") for c in expected],
                     rescued.alias("_rescued_data"), "_file")


def shape_jsonl(df: DataFrame, source_name: str) -> DataFrame:
    """Parse one JSON object per text line; keep the original line byte-for-byte.

    * unexpected top-level keys -> `_rescued_data` (JSON array of key names)
    * unparseable line          -> `_corrupt_record` (original text)
    """
    cfg = SOURCES[source_name]
    expected = cfg["expected_columns"]
    schema: StructType = cfg["json_schema"]

    lines = (df.where(F.trim("value") != "")
               .withColumnRenamed("value", "raw_payload")
               .withColumn("_keys", F.json_object_keys("raw_payload")))
    parsed = F.from_json("raw_payload", schema)
    extra_keys = F.array_except("_keys", F.array(*[F.lit(c) for c in expected]))
    return lines.select(
        *[parsed[c].alias(c) for c in expected],
        F.when(F.size(extra_keys) > 0, F.to_json(extra_keys)).alias("_rescued_data"),
        F.when(F.col("_keys").isNull(), F.col("raw_payload")).alias("_corrupt_record"),
        "raw_payload",
        "_file",
    )


# --------------------------------------------------------------------------- #
# 3. Audit columns
# --------------------------------------------------------------------------- #
def _as_text(df: DataFrame, c: str) -> Column:
    col = F.col(f"`{c}`")
    return (F.to_json(col) if isinstance(df.schema[c].dataType, (StructType, ArrayType))
            else col.cast("string"))


def batch_id_expr(source_name: str) -> Column:
    """Deterministic batch id = one file version: <source>-<file mod time>-<path hash>.

    The same file always gets the same batch id, so a pipeline full refresh
    reproduces identical ids and any row is traceable to the exact file version.
    """
    return F.concat_ws(
        "-",
        F.lit(source_name),
        F.date_format("_file.file_modification_time", "yyyyMMdd'T'HHmmss"),
        F.substring(F.sha2(F.col("_file.file_path"), 256), 1, 8),
    )


def add_audit_columns(df: DataFrame, source_name: str) -> DataFrame:
    cfg = SOURCES[source_name]
    business = [c for c in df.columns if c not in ("_file", "raw_payload")]
    # NULL is encoded explicitly so that NULL and '' produce different hashes.
    hash_input = F.concat_ws("\u0001", *[F.coalesce(_as_text(df, c), F.lit("<NULL>"))
                                         for c in business])

    status = F.lit(LOAD_STATUS_LOADED)
    if "_rescued_data" in df.columns:
        status = F.when(F.col("_rescued_data").isNotNull(), LOAD_STATUS_SCHEMA_DRIFT).otherwise(status)
    if "_corrupt_record" in df.columns:
        status = F.when(F.col("_corrupt_record").isNotNull(), LOAD_STATUS_MALFORMED).otherwise(status)

    return (df
            .withColumn("source_file_name", F.col("_file.file_path"))
            .withColumn("source_file_mod_ts", F.col("_file.file_modification_time"))
            .withColumn("ingestion_ts", F.current_timestamp())
            .withColumn("batch_id", batch_id_expr(source_name))
            .withColumn("source_system", F.lit(cfg["source_system"]))
            .withColumn("record_hash", F.sha2(hash_input, 256))
            .withColumn("load_status", status)
            .drop("_file"))


def build_bronze(df: DataFrame, source_name: str) -> DataFrame:
    """Full Bronze transformation for one source, from raw file DataFrame to table rows."""
    fmt = SOURCES[source_name]["format"]
    df = with_file_metadata(df)
    df = shape_csv(df, source_name) if fmt == "csv" else shape_jsonl(df, source_name)
    return add_audit_columns(df, source_name)


# --------------------------------------------------------------------------- #
# 4. Per-file summary (feeds ops_bronze_file_summary)
# --------------------------------------------------------------------------- #
def file_summary(df: DataFrame, source_name: str) -> DataFrame:
    return (df.groupBy("source_system", "source_file_name", "source_file_mod_ts", "batch_id")
              .agg(F.count(F.lit(1)).alias("rows_written"),
                   F.sum((F.col("load_status") == LOAD_STATUS_LOADED).cast("int")).alias("rows_loaded"),
                   F.sum((F.col("load_status") == LOAD_STATUS_SCHEMA_DRIFT).cast("int"))
                    .alias("rows_schema_drift"),
                   F.sum((F.col("load_status") == LOAD_STATUS_MALFORMED).cast("int")).alias("rows_malformed"),
                   F.min("ingestion_ts").alias("first_ingestion_ts"),
                   F.max("ingestion_ts").alias("last_ingestion_ts"))
              .withColumn("source_name", F.lit(source_name))
              .withColumn("target_table", F.lit(SOURCES[source_name]["target_table"])))
