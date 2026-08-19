"""
Shared pytest fixtures for the whole suite.

One local SparkSession is created per test session and reused. Booting a JVM
costs several seconds, so a per-module session makes the suite unusably slow —
and worse, two modules each calling getOrCreate() get the *same* underlying
session, so whichever one tears down first stops Spark out from under the
other. Session scope makes that ownership explicit and unambiguous.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("job_market_analytics_tests")
        # Default is 200 shuffle partitions — absurd overhead for the handful
        # of rows these tests use, and the single biggest local-test speedup.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
