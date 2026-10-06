"""Helpers: run the real pipeline on a landing folder, load the DQ answer keys, reconcile detections."""
import os
import shutil

from pyspark.sql import functions as F

from pipeline_runner import SRC_DIR, run_pipeline

SOURCES = [("suppliers", "csv"), ("products", "csv"), ("purchase_orders", "csv"), ("shipment_events", "json")]


def run_on_landing(spark, landing: str, as_of: str = "2026-10-05") -> dict:
    """Runs pipelines/medallion/*.py on a landing folder; returns {table: DataFrame}."""
    return run_pipeline(spark, {"pipeline.src_path": os.path.abspath(SRC_DIR), "bronze.landing_root": landing,
                                "gold.as_of_date": as_of, "spark.sql.session.timeZone": "UTC"})


def make_landing(data_dir: str, landing) -> str:
    for src, ext in SOURCES:
        (landing / src).mkdir()
        shutil.copy(os.path.join(data_dir, f"{src}.{ext}"), landing / src)
    return str(landing)


def answer_keys(spark, data_dir):
    frames = [spark.read.option("header", True).option("multiLine", True).option("escape", '"')
              .csv(os.path.join(data_dir, f"{src}_errors.csv"))
              .select(F.lit(src).alias("entity"),
                      F.col("source_line_number").cast("int").alias("source_line_number"),
                      F.col("dq_rule").alias("rule"))
              for src, _ in SOURCES]
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


def reconcile(issues_df, keys_df):
    """Recall per rule: answer-key defects (entity + line + rule) found in ops_dq_issues."""
    found = issues_df.select("entity", "source_line_number", "rule").distinct().withColumn("detected", F.lit(True))
    joined = keys_df.join(found, ["entity", "source_line_number", "rule"], "left")
    recall = (joined.groupBy("entity", "rule")
              .agg(F.count(F.lit(1)).alias("expected"),
                   F.sum(F.coalesce(F.col("detected"), F.lit(False)).cast("int")).alias("detected")))
    extra = found.join(keys_df.select("entity", "source_line_number").distinct(),
                       ["entity", "source_line_number"], "left_anti")
    return recall, joined.where(F.col("detected").isNull()), extra
