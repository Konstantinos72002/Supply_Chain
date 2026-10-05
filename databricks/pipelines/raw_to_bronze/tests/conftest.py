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
             .getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture
def data_dir():
    return DATA_DIR
