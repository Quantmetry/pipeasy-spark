# -*- coding: utf-8 -*-
from pyspark.ml import Pipeline
from .transformers import (
    set_transformer_in_out,
    ColumnDropper,
    ColumnRenamer,
)


def build_pipeline(column_transformers):
    """Create a dataframe transformation pipeline.

    The created pipeline can be used to apply successive transformations
    on a spark dataframe. The transformations are intended to be applied
    per column.

    Example
    -------

        >>> df = titanic.select('Survived', 'Sex', 'Age').dropna()
        >>> df.show(2)
        +--------+------+----+
        |Survived|   Sex| Age|
        +--------+------+----+
        |       0|  male|22.0|
        |       1|female|38.0|
        +--------+------+----+
        >>> pipeline = build_pipeline({
                # 'Survived' : this variable is not modified, it can also be omitted from the dict
                'Survived': [],
                'Sex': [StringIndexer(), OneHotEncoderEstimator(dropLast=False)],
                # 'Age': a VectorAssembler must be applied before the StandardScaler
                # as the latter only accepts vectors as input.
                'Age': [VectorAssembler(), StandardScaler()]
            })
        >>> trained_pipeline = pipeline.fit(df)
        >>> trained_pipeline.transform(df).show(2)
        +--------+-------------+--------------------+
        |Survived|          Sex|                 Age|
        +--------+-------------+--------------------+
        |       0|(2,[0],[1.0])|[1.5054181442954726]|
        |       1|(2,[1],[1.0])| [2.600267703783089]|
        +--------+-------------+--------------------+


    Parameters
    ----------
    column_transformers: dict(str -> list)
        key (str): column name; value (list): transformer instances
        (typically instances of pyspark.ml.feature transformers)

    Returns
    -------
    pipeline: a pyspark.ml.Pipeline instance (untrained)

    """
    stages = []

    for column, transformers in column_transformers.items():
        temp_column_names = [column]

        for transformer in transformers:
            previous_column = temp_column_names[-1]
            next_column = previous_column + '_' + transformer.__class__.__name__
            temp_column_names.append(next_column)

            transformer = set_transformer_in_out(transformer, previous_column, next_column)
            stages.append(transformer)

        # all the temporary columns should be dropped except the last one
        stages.append(ColumnDropper(inputCols=temp_column_names[:-1]))
        # the last temporary column should be renamed to the original name
        stages.append(ColumnRenamer(
            temp_column_names[-1], temp_column_names[0]
        ))

    # Create a Pipeline
    pipeline = Pipeline(stages=stages)
    return pipeline
