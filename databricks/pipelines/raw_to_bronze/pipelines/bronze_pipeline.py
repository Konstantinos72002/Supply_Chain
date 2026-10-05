"""
Bronze layer - Lakeflow Declarative Pipeline (ETL pipeline) - Team 4 Supply Chain.

Pipeline settings (see README / resources/pipeline.yml):
    Default catalog : data2026acc123456
    Default schema  : team4
    Configuration   : bronze.landing_root = /Volumes/data2026acc123456/team4/landing
                      bronze.src_path     = /Workspace/Users/<you>/team4_pipeline/src

What the pipeline engine gives us for free:
    * Auto Loader checkpoints -> each file is ingested exactly once (idempotent reruns)
    * Incremental processing   -> only new files are read on each update
    * Retries, lineage graph, event log with row counts and expectation metrics
"""
import sys

import dlt
from pyspark.sql import functions as F  # noqa: F401  (handy for ad-hoc edits)

sys.path.append(spark.conf.get("bronze.src_path"))  # noqa: F821 - `spark` is provided by the pipeline

from bronze.config import (BRONZE_TABLE_PROPERTIES, DEFAULT_LANDING_ROOT,  # noqa: E402
                           SOURCES, csv_schema)
from bronze.transforms import build_bronze, file_summary  # noqa: E402

LANDING_ROOT = spark.conf.get("bronze.landing_root", DEFAULT_LANDING_ROOT)  # noqa: F821


def _reader(source_name: str):
    """Auto Loader stream for one landing folder."""
    cfg = SOURCES[source_name]
    path = f"{LANDING_ROOT}/{cfg['folder']}"
    if cfg["format"] == "csv":
        return (spark.readStream.format("cloudFiles")  # noqa: F821
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.schemaEvolutionMode", "rescue")  # never fail, never evolve
                .option("rescuedDataColumn", "_rescued_data")        # drift / mismatches go here
                .option("pathGlobFilter", "*.csv")
                .option("header", "true")
                .option("multiLine", "true")
                .option("quote", '"').option("escape", '"')
                .schema(csv_schema(source_name))
                .load(path))
    # NDJSON is read as text so the original line is preserved and malformed lines survive
    return (spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "text")
            .option("pathGlobFilter", "*.json")
            .load(path))


def define_bronze_table(source_name: str):
    cfg = SOURCES[source_name]
    key_present = " AND ".join(f"{k} IS NOT NULL" for k in cfg["key_columns"])

    # Expectations in Bronze only WARN (rows are kept); results appear in the event log.
    expectations = {
        "no_schema_drift": "_rescued_data IS NULL",
        "primary_key_present": key_present,
    }
    if cfg["format"] == "jsonl":
        expectations["valid_json_line"] = "_corrupt_record IS NULL"

    @dlt.table(name=cfg["target_table"], comment=cfg["comment"],
               table_properties=BRONZE_TABLE_PROPERTIES)
    @dlt.expect_all(expectations)
    def bronze_table():
        return build_bronze(_reader(source_name), source_name)

    return bronze_table


for _name in SOURCES:
    define_bronze_table(_name)


@dlt.table(
    name="ops_bronze_file_summary",
    comment="One row per ingested file version: rows written by load_status, "
            "first/last ingestion time. Reconciliation evidence for Bronze.",
)
def ops_bronze_file_summary():
    frames = [file_summary(spark.read.table(cfg["target_table"]), name)  # noqa: F821
              for name, cfg in SOURCES.items()]
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out
