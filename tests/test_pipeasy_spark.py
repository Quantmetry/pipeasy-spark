# coding: utf8
"""Tests for `pipeasy_spark` package."""
import pytest

import pyspark

from pipeasy_spark import pipeasy_spark


def test_dummy():
    assert True


@pytest.fixture(scope='session')
def spark():
    spark = pyspark.sql.SparkSession \
        .builder \
        .master('local[1]') \
        .appName("Spark session for unit tests") \
        .getOrCreate()
    return spark


def test_spark(spark):
    df = spark.createDataFrame([
        ('Benjamin', 178.1, 0),
        ('Omar', 178.1, 1),
    ], schema=['name', 'height', 'target'])

    pipeline = pipeasy_spark.map_by_dtypes(df, 'target')

    assert isinstance(pipeline, pyspark.ml.Pipeline)
