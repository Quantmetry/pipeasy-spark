from unittest.mock import patch

from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoderEstimator,
    VectorAssembler,
    StandardScaler,
)

import pipeasy_spark


def test_build_pipeline_by_dtypes_with_obvious_transformers(
    spark, string_transformers, number_transformers
):
    df = spark.createDataFrame(
        [("Benjamin", 178.1, 0), ("Omar", 178.1, 1)],
        schema=["name", "height", "target"],
    )

    pipeline = pipeasy_spark.build_pipeline_by_dtypes(
        df,
        exclude_columns=["target"],
        string_transformers=[StringIndexer(), OneHotEncoderEstimator()],
        numeric_transformers=[VectorAssembler(), StandardScaler()],
    )

    trained_pipeline = pipeline.fit(df)
    df_transformed = trained_pipeline.transform(df)

    def get_target_values(dataframe):
        return dataframe.rdd.map(lambda row: row["target"]).collect()

    assert set(df.columns) == set(df_transformed.columns)
    assert df.count() == df_transformed.count()
    assert get_target_values(df) == get_target_values(df_transformed)


def test_build_default_pipeline_calls_other_method(spark):
    df = spark.createDataFrame(
        [("Benjamin", 178.1, 0), ("Omar", 178.1, 1)],
        schema=["name", "height", "target"],
    )

    with patch("pipeasy_spark.convenience.build_pipeline_by_dtypes") as fake_func:
        pipeasy_spark.build_default_pipeline(df, exclude_columns="target")
        assert fake_func.called
        args, kwargs = fake_func.call_args
        assert args == (df,)
        assert all(
            arg in kwargs
            for arg in [
                "exclude_columns",
                "string_transformers",
                "numeric_transformers",
            ]
        )
        assert kwargs["exclude_columns"] == "target"
