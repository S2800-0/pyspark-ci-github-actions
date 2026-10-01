import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from pyspark_job import clean_data

SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
    ]
)


@pytest.fixture(scope="session")
def spark():
    # Spark workers must run the same Python as the driver.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("clean-data-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def names(df):
    return sorted(row["name"] for row in df.collect())


def test_valid_records_are_kept(spark):
    df = spark.createDataFrame([("Alice", 100.0), ("Bob", 0.01)], SCHEMA)
    assert names(clean_data(df)) == ["Alice", "Bob"]


def test_non_positive_amounts_are_removed(spark):
    df = spark.createDataFrame(
        [("Alice", 100.0), ("Zero", 0.0), ("Negative", -50.0)], SCHEMA
    )
    assert names(clean_data(df)) == ["Alice"]


def test_null_names_are_removed(spark):
    df = spark.createDataFrame([("Alice", 100.0), (None, 200.0)], SCHEMA)
    assert names(clean_data(df)) == ["Alice"]


def test_null_amounts_are_removed(spark):
    df = spark.createDataFrame([("Alice", 100.0), ("NoAmount", None)], SCHEMA)
    assert names(clean_data(df)) == ["Alice"]


def test_amount_with_tax_is_calculated(spark):
    df = spark.createDataFrame([("Alice", 100.0), ("Bob", 49.99)], SCHEMA)
    result = {r["name"]: r["amount_with_tax"] for r in clean_data(df).collect()}
    assert result["Alice"] == pytest.approx(120.0)
    assert result["Bob"] == pytest.approx(59.988)


def test_output_schema(spark):
    df = spark.createDataFrame([("Alice", 100.0)], SCHEMA)
    assert clean_data(df).columns == ["name", "amount", "amount_with_tax"]


def test_all_invalid_returns_empty(spark):
    df = spark.createDataFrame([(None, 10.0), ("Zero", 0.0)], SCHEMA)
    assert clean_data(df).count() == 0
