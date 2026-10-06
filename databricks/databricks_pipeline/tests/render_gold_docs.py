"""Regenerate docs/data_model.md:  BRONZE_TEST_DATA=/path/to/files python tests/render_gold_docs.py"""
import os
import sys
import tempfile
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "src"), HERE]

from pyspark.sql import SparkSession  # noqa: E402

from gold.render_docs import render  # noqa: E402
from silver_harness import make_landing, run_on_landing  # noqa: E402

if __name__ == "__main__":
    spark = (SparkSession.builder.master("local[2]").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "true").config("spark.driver.memory", "2g").getOrCreate())
    landing = make_landing(os.environ["BRONZE_TEST_DATA"], Path(tempfile.mkdtemp()))
    tables = run_on_landing(spark, landing)["tables"]
    out = os.path.join(HERE, "..", "docs", "data_model.md")
    with open(out, "w") as fh:
        fh.write(render({t: df.columns for t, df in tables.items() if t.startswith("gold_")}))
    print("written", out)
