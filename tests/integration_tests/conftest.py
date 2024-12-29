# tests/integration_tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    # Because this file is not a Databricks notebook,
    # we must manually create a Spark session.
    spark = SparkSession.builder.appName('integration-tests').getOrCreate()
    return spark

@pytest.fixture
def get_dbutils(spark):
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    return dbutils
