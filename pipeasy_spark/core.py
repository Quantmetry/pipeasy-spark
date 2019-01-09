# -*- coding: utf-8 -*-

from pyspark.ml import Pipeline
from .transformers import (
    set_transformer_in_out,
    ColumnDropper,
    ColumnRenamer,
    FeatureBuilder,
)

"""The pipeasy-spark package provides a set of convenience classes and functions that
make it easier to map each column of a Spark dataframe (or subsets of columns) to
user-specified transformations. Increasingly complex features are provided:


map_by_column()     allows mapping transformations at a more detailed level. Each column
                    of the dataframe (or subset thereof) can be assigned a specific
                    sequence of transformations.

Each function returns a pyspark.ml Pipeline object.

"""


def map_by_column(columns_mapping, target_name=None):
    """Create a dataframe transformation pipeline.

    Example
    -------

        >>> df.show(1)
        ....
        >>> pipeline = map_by_column({
                'species': [StringIndexer(), OneHotEncoder()],
                'sepal_length': [StandardScaler()],
        })
        >>> df_transormed = pipeline.fit_transform(df)


    Parameters
    ----------
        columns_mapping: dict
        target_name: str
            name of the target column that will be converted using StringIndexer()

    Returns
    -------
        pipeline: a pyspark pipeline
    """
    stages = []
    columns_to_drop = []
    columns_to_rename = []

    for column, transformers in columns_mapping.items():
        if transformers:
            temp_column_names = [column]
            for transformer in transformers:
                previous_column = temp_column_names[-1]
                next_column = previous_column + '_' + transformer.__class__.__name__
                temp_column_names.append(next_column)

                transformer = set_transformer_in_out(transformer, previous_column, next_column)
                stages.append(transformer)

            # all the temporary columns should be dropped except the last one
            columns_to_drop += temp_column_names[:-1]
            # the last temporary column should be renamed to the original name
            columns_to_rename.append(
                (temp_column_names[-1], temp_column_names[0])
            )
    for column in columns_to_drop:
        stages.append(ColumnDropper(inputCol=column))
    for original, new in columns_to_rename:
        stages.append(ColumnRenamer(original, new))

    if target_name:
        stages.append(FeatureBuilder(targetCol=target_name))
    # Create a Pipeline
    pipeline = Pipeline(stages=stages)
    return pipeline
