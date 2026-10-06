import os
import sys

import pytest
from pyspark.sql import SparkSession

# Worker Spark harus memakai interpreter yang sama dengan driver.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("orders-stream-tests")
        .config("spark.sql.session.timeZone", "Asia/Jakarta")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
