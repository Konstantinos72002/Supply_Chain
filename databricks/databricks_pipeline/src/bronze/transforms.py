"""
Pure PySpark transformations used by the Bronze ETL pipeline.

They work on batch AND streaming DataFrames, so they are unit-tested locally
(tests/) and reused unchanged in pipelines/medallion/01_bronze.py.

Flow for every source:
    whole file as text  ->  with_file_metadata()  ->  shape_csv() | shape_jsonl()
                        ->  add_audit_columns()   ->  Bronze table
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType

from bronze.config import (EXCLUDED_FILE_SUFFIXES, LOAD_STATUS_LOADED,
                           LOAD_STATUS_MALFORMED, LOAD_STATUS_SCHEMA_DRIFT,
                           SOURCES)

# Audit columns required by business_brief.md section 5, plus source_line_number (see docs/design_notes.md)
AUDIT_COLUMNS = ["source_file_name", "source_line_number", "ingestion_ts", "batch_id", "source_system",
                 "record_hash", "load_status"]


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
# 2. Lines -> records (no business transformation - values stay strings)
# --------------------------------------------------------------------------- #
# Files are read WHOLE as text (one row per file) and split into lines here.
# posexplode is streaming-safe and gives the exact source line number, which
# makes every Bronze row traceable to file + line (and lets Silver resolve
# duplicates by "first occurrence in the file").
# Limitation (documented): quoted CSV values must not contain line breaks, and
# a single file should stay well below ~1 GB.
CSV_MAX_FIELDS = 64                                   # widest CSV we accept
_CSV_SPLIT_REGEX = ',(?=(?:[^"]*"[^"]*")*[^"]*$)'     # commas outside quotes
_WIDE_CSV_SCHEMA = ", ".join(f"c{i} STRING" for i in range(CSV_MAX_FIELDS))


def explode_lines(df: DataFrame) -> DataFrame:
    """value (whole file) -> one row per non-empty line with `source_line_number`."""
    lines = F.split(F.regexp_replace("value", "^\ufeff", ""), "\r?\n")
    # Header is extracted BEFORE the explode: otherwise Spark evaluates it after the
    # Generate and every exploded row would carry the whole file (quadratic memory).
    return (df.select("_file", lines.alias("_lines"))
              .select("_file", "_lines", F.element_at("_lines", 1).alias("_header_line"))
              .select("_file", "_header_line", F.posexplode("_lines").alias("_pos", "raw_payload"))
              .withColumn("source_line_number", (F.col("_pos") + 1).cast("int"))
              .where(F.trim("raw_payload") != "")
              .drop("_pos", "_lines"))


def _csv_fields(col: str) -> Column:
    parsed = F.from_csv(F.col(col), _WIDE_CSV_SCHEMA, {"quote": '"', "escape": '"'})
    return F.array(*[parsed[f"c{i}"] for i in range(CSV_MAX_FIELDS)])


def shape_csv(df: DataFrame, source_name: str) -> DataFrame:
    """Map CSV lines to contract columns BY HEADER NAME.

    * extra / missing columns -> `_rescued_data` (JSON), status SCHEMA_DRIFT
    * wrong number of fields   -> status MALFORMED (original line kept in raw_payload)
    """
    expected = SOURCES[source_name]["expected_columns"]
    exp_arr = "array(" + ", ".join(f"'{c}'" for c in expected) + ")"
    df = (explode_lines(df)
          .where(F.col("source_line_number") > 1)                       # skip header
          .withColumn("_hdr", F.filter(F.transform(_csv_fields("_header_line"), lambda x: F.trim(x)),
                                       lambda x: x.isNotNull()))
          .withColumn("_vals", _csv_fields("raw_payload"))
          .withColumn("_n_fields", F.size(F.split("raw_payload", _CSV_SPLIT_REGEX, -1))))

    def value_of(name: str) -> Column:
        # array_position = 0 when the column is missing; index 0 is an error even for try_element_at
        return F.expr(f"CASE WHEN array_position(_hdr, '{name}') > 0 "
                      f"THEN try_element_at(_vals, CAST(array_position(_hdr, '{name}') AS INT)) END")

    extra = F.expr(f"array_except(_hdr, {exp_arr})")
    missing = F.expr(f"array_except({exp_arr}, _hdr)")
    extra_map = F.expr(
        f"map_from_arrays(array_except(_hdr, {exp_arr}), "
        f"transform(array_except(_hdr, {exp_arr}), "
        f"n -> try_element_at(_vals, CAST(array_position(_hdr, n) AS INT))))")
    rescued = F.when((F.size(extra) > 0) | (F.size(missing) > 0),
                     F.to_json(F.struct(extra_map.alias("extra_columns"),
                                        missing.alias("missing_columns"))))
    malformed = F.col("_n_fields") != F.size("_hdr")
    return df.select(*[value_of(c).alias(c) for c in expected],
                     rescued.alias("_rescued_data"), malformed.alias("_malformed"),
                     "raw_payload", "source_line_number", "_file")


def shape_jsonl(df: DataFrame, source_name: str) -> DataFrame:
    """Parse one JSON object per line; keep the original line byte-for-byte.

    * unexpected top-level keys -> `_rescued_data` (JSON array of key names)
    * unparseable line          -> status MALFORMED (original line kept in raw_payload)
    """
    cfg = SOURCES[source_name]
    expected = cfg["expected_columns"]
    schema: StructType = cfg["json_schema"]

    lines = explode_lines(df).withColumn("_keys", F.json_object_keys("raw_payload"))
    parsed = F.from_json("raw_payload", schema)
    extra_keys = F.array_except("_keys", F.array(*[F.lit(c) for c in expected]))
    return lines.select(
        *[parsed[c].alias(c) for c in expected],
        F.when(F.size(extra_keys) > 0, F.to_json(extra_keys)).alias("_rescued_data"),
        F.col("_keys").isNull().alias("_malformed"),
        "raw_payload", "source_line_number", "_file",
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
    business = cfg["expected_columns"] + ["_rescued_data"]
    # NULL is encoded explicitly so that NULL and '' produce different hashes.
    hash_input = F.concat_ws("\u0001", *[F.coalesce(_as_text(df, c), F.lit("<NULL>")) for c in business])
    status = (F.when(F.col("_malformed"), LOAD_STATUS_MALFORMED)
               .when(F.col("_rescued_data").isNotNull(), LOAD_STATUS_SCHEMA_DRIFT)
               .otherwise(LOAD_STATUS_LOADED))
    return df.select(
        *business, "raw_payload",
        F.col("_file.file_path").alias("source_file_name"),
        "source_line_number",
        F.current_timestamp().alias("ingestion_ts"),
        batch_id_expr(source_name).alias("batch_id"),
        F.lit(cfg["source_system"]).alias("source_system"),
        F.sha2(hash_input, 256).alias("record_hash"),
        status.alias("load_status"),
    )


def build_bronze(df: DataFrame, source_name: str) -> DataFrame:
    """Full Bronze transformation for one source, from raw file DataFrame to table rows."""
    fmt = SOURCES[source_name]["format"]
    df = with_file_metadata(df)
    df = shape_csv(df, source_name) if fmt == "csv" else shape_jsonl(df, source_name)
    return add_audit_columns(df, source_name)


# --------------------------------------------------------------------------- #
# 4. Ingestion log (assignment 6.1: input, written and rejected counts per batch)
#    Duration and failure details come from the pipeline event log.
# --------------------------------------------------------------------------- #
def ingestion_log(df: DataFrame, source_name: str) -> DataFrame:
    return (df.groupBy("batch_id", "source_file_name", "source_system")
              .agg(F.count(F.lit(1)).alias("input_count"),
                   F.sum((F.col("load_status") != LOAD_STATUS_MALFORMED).cast("int")).alias("written_count"),
                   F.sum((F.col("load_status") == LOAD_STATUS_MALFORMED).cast("int")).alias("rejected_count"),
                   F.sum((F.col("load_status") == LOAD_STATUS_SCHEMA_DRIFT).cast("int")).alias("schema_drift_count"),
                   F.max("ingestion_ts").alias("ingestion_ts"))
              .withColumn("target_table", F.lit(SOURCES[source_name]["target_table"])))
