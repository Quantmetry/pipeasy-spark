import pytest
import pyspark
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoderEstimator,
    VectorAssembler,
    StandardScaler,
)


@pytest.fixture(scope='session')
def spark():
    return pyspark.sql.SparkSession \
        .builder \
        .master('local[1]') \
        .appName("Spark session for unit tests") \
        .getOrCreate()


@pytest.fixture()
def string_transformers(spark):
    return [
        StringIndexer(),
        OneHotEncoderEstimator(),
    ]


@pytest.fixture()
def number_transformers(spark):
    return [
        VectorAssembler(),
        StandardScaler(),
    ]
