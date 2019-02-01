import inspect

import pytest
import pyspark
from pyspark.ml import Pipeline

from pipeasy_spark.transformers import (
    set_transformer_in_out,
    ColumnDropper,
    ColumnRenamer,
)


def test_works_on_some_pyspark_transformers(pyspark_transformers):
    """Test the function set_transformer_in_out.

    We want to make sure that this function works on a few pyspark transformers.

    A transformer is considered 'invalid' if one cannot set its inputCol.

        Valid Transformers:
            <class 'pyspark.ml.feature.Binarizer'>
            <class 'pyspark.ml.feature.BucketedRandomProjectionLSH'>
            <class 'pyspark.ml.feature.Bucketizer'>
            <class 'pyspark.ml.feature.CountVectorizer'>
            <class 'pyspark.ml.feature.DCT'>
            <class 'pyspark.ml.feature.ElementwiseProduct'>
            <class 'pyspark.ml.feature.FeatureHasher'>
            <class 'pyspark.ml.feature.HashingTF'>
            <class 'pyspark.ml.feature.IDF'>
            <class 'pyspark.ml.feature.Imputer'>
            <class 'pyspark.ml.feature.IndexToString'>
            <class 'pyspark.ml.feature.MaxAbsScaler'>
            <class 'pyspark.ml.feature.MinHashLSH'>
            <class 'pyspark.ml.feature.MinMaxScaler'>
            <class 'pyspark.ml.feature.NGram'>
            <class 'pyspark.ml.feature.Normalizer'>
            <class 'pyspark.ml.feature.OneHotEncoder'>
            <class 'pyspark.ml.feature.OneHotEncoderEstimator'>
            <class 'pyspark.ml.feature.PCA'>
            <class 'pyspark.ml.feature.PolynomialExpansion'>
            <class 'pyspark.ml.feature.QuantileDiscretizer'>
            <class 'pyspark.ml.feature.RegexTokenizer'>
            <class 'pyspark.ml.feature.StandardScaler'>
            <class 'pyspark.ml.feature.StopWordsRemover'>
            <class 'pyspark.ml.feature.StringIndexer'>
            <class 'pyspark.ml.feature.Tokenizer'>
            <class 'pyspark.ml.feature.VectorAssembler'>
            <class 'pyspark.ml.feature.VectorIndexer'>
            <class 'pyspark.ml.feature.VectorSizeHint'>
            <class 'pyspark.ml.feature.VectorSlicer'>
            <class 'pyspark.ml.feature.Word2Vec'>
        Invalid Transformers:
            <class 'pyspark.ml.feature.ChiSqSelector'>
            <class 'pyspark.ml.feature.LSHParams'>
            <class 'pyspark.ml.feature.RFormula'>
            <class 'pyspark.ml.feature.SQLTransformer'>

    """
    valid_transformers = []
    invalid_transformers = []
    for transformer in pyspark_transformers:
        try:
            instance = transformer()
            set_transformer_in_out(instance, 'input', 'output')
            valid_transformers.append(transformer)
        except ValueError:
            invalid_transformers.append(transformer)

    assert len(valid_transformers) > 10
    assert pyspark.ml.feature.VectorAssembler in valid_transformers
    print('Valid Transformers:')
    for t in valid_transformers:
        print('\t' + str(t))

    assert len(invalid_transformers) > 0
    assert pyspark.ml.feature.SQLTransformer in invalid_transformers
    print('Invalid Transformers:')
    for t in invalid_transformers:
        print('\t' + str(t))


def test_ColumnDropper(spark):
    df = spark.createDataFrame([(0, 0, 0)], schema=['keep', 'remove', 'also_remove'])
    df_transformed = (
        Pipeline(stages=[
            ColumnDropper(inputCols=['remove', 'also_remove'])
        ])
        .fit(df)
        .transform(df)
    )
    assert df_transformed.columns == ['keep']


def test_ColumnRenamer(spark):
    df = spark.createDataFrame([(0, 0)], schema=['keep', 'old_name'])
    df_transformed = (
        Pipeline(stages=[
            ColumnRenamer(inputCol='old_name', outputCol='new_name')
        ])
        .fit(df)
        .transform(df)
    )
    assert set(df_transformed.columns) == {'keep', 'new_name'}


@pytest.fixture
def pyspark_transformers(spark):
    """list all transformer classes in pyspark.ml.feature"""
    def is_transformer(obj):
        return (
            inspect.getmodule(obj) == pyspark.ml.feature
            and inspect.isclass(obj)
            and not issubclass(obj, pyspark.ml.feature.JavaModel))

    all_objects = [obj for name, obj in inspect.getmembers(pyspark.ml.feature)]
    transformers = [obj for obj in all_objects if is_transformer(obj)]
    return transformers
