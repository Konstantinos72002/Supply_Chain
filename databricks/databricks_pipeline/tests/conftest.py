import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

DATA_DIR = os.environ.get("BRONZE_TEST_DATA",
                          os.path.join(os.path.dirname(__file__), "..", "data"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("bronze-tests")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.driver.memory", "2g")
             .config("spark.sql.ansi.enabled", "true")      # same as Databricks serverless
             .getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def data_dir():
    return DATA_DIR


@pytest.fixture(scope="session")
def pipeline(spark, data_dir, tmp_path_factory):
    """The real one-pipeline run (pipelines/medallion/*.py) on the generated datasets, once per session."""
    from silver_harness import make_landing, run_on_landing
    landing = make_landing(data_dir, tmp_path_factory.mktemp("landing"))
    return run_on_landing(spark, landing)


@pytest.fixture(scope="session")
def tables(pipeline):
    return pipeline["tables"]
