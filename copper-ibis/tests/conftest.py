"""Shared pytest fixtures.

The SparkSession here is a plain local one -- no Databricks, no Lakeflow. That is only
possible because everything in ``turbine_pipeline.lib`` is pure DataFrame-in/DataFrame-out.
"""

from __future__ import annotations

import os
import sys

import pytest

# Both sides of the Py4J bridge must be the same interpreter.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
# On Windows the driver can resolve to a non-loopback name (e.g. host.docker.internal),
# and the Python workers then fail to connect back with "connection abort: recv failed".
# Pinning to loopback is the fix; harmless everywhere else.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("turbine-pipeline-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Keep test output readable; Spark is extremely chatty by default.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
