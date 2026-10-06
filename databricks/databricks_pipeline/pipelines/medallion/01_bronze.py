"""
Team 4 Supply Chain Disruption Analytics - ONE Lakeflow ETL pipeline, 1 of 3 source files.

    01_bronze.py  landing files (Volume)  ->  bronze_*            Auto Loader streaming tables
    02_silver.py  bronze_*                ->  silver_*, ops_dq_*  materialized views
    03_gold.py    silver_*                ->  gold_*              materialized views (star schema)

The three files belong to the same pipeline: Lakeflow builds one dependency graph across them,
so a single update runs raw -> Bronze -> Silver -> Gold in the right order.

Pipeline configuration (resources/pipelines.yml):
    pipeline.src_path          /Workspace/Users/<you>/team4_supply_chain/src
    bronze.landing_root        /Volumes/data2026acc123456/team4/landing
    gold.as_of_date            optional yyyy-MM-dd = brief "snapshot_date" (empty = today)
    spark.sql.session.timeZone UTC
"""

# Bronze layer - Lakeflow Declarative Pipeline (ETL pipeline) - Team 4 Supply Chain.
#
# What the pipeline engine gives us for free:
#     * Auto Loader checkpoints -> each file is ingested exactly once (idempotent reruns)
#     * Incremental processing   -> only new files are read on each update
#     * Retries, lineage graph, event log with row counts and expectation metrics

import sys

import dlt
from pyspark.sql import functions as F  # noqa: F401  (handy for ad-hoc edits)

_SRC = spark.conf.get("pipeline.src_path")  # noqa: F821 - `spark` is provided by the pipeline
if _SRC not in sys.path:
    sys.path.append(_SRC)

from bronze.config import BRONZE_TABLE_PROPERTIES, DEFAULT_LANDING_ROOT, SOURCES  # noqa: E402
from bronze.transforms import build_bronze, ingestion_log  # noqa: E402

LANDING_ROOT = spark.conf.get("bronze.landing_root", DEFAULT_LANDING_ROOT)  # noqa: F821


def _reader(source_name: str):
    """Auto Loader stream: one row per file (whole text), split into lines in build_bronze()."""
    cfg = SOURCES[source_name]
    ext = "csv" if cfg["format"] == "csv" else "json"
    return (spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "text")
            .option("wholetext", "true")
            .option("pathGlobFilter", f"*.{ext}")
            .load(f"{LANDING_ROOT}/{cfg['folder']}"))


def define_bronze_table(source_name: str):
    cfg = SOURCES[source_name]
    key_present = " AND ".join(f"{k} IS NOT NULL" for k in cfg["key_columns"])

    # Expectations in Bronze only WARN (rows are kept); results appear in the event log.
    expectations = {
        "no_schema_drift": "_rescued_data IS NULL",
        "primary_key_present": key_present,
    }
    expectations["parseable_record"] = "load_status <> 'MALFORMED'"

    @dlt.table(name=cfg["target_table"], comment=cfg["comment"],
               table_properties=BRONZE_TABLE_PROPERTIES)
    @dlt.expect_all(expectations)
    def bronze_table():
        return build_bronze(_reader(source_name), source_name)

    return bronze_table


for _name in SOURCES:
    define_bronze_table(_name)


@dlt.table(
    name="ops_bronze_ingestion_log",
    comment="Assignment 6.1: one row per ingested file version (batch) with input, written and rejected "
            "counts. Rejected = MALFORMED rows (kept in Bronze with raw_payload, excluded from Silver). "
            "Duration and failure details: pipeline event log ops_pipeline_event_log.",
)
def ops_bronze_ingestion_log():
    frames = [ingestion_log(spark.read.table(cfg["target_table"]), name)  # noqa: F821
              for name, cfg in SOURCES.items()]
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out
