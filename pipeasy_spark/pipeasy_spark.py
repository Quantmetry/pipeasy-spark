# -*- coding: utf-8 -*-

"""Main module."""
import pyspark
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    VectorAssembler,
)

"""The pipeasy-spark package provides a set of convenience classes and functions that
make it easier to map each column of a Spark dataframe (or subsets of columns) to
user-specified transformations. Increasingly complex features are provided:

map_by_dtypes()     allows a simple mapping of features to user-specified transformations
                    according to their dtypes. Each dtype is assigned the sequence of
                    transformations passed in the arguments as dictionaries.

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


def set_transformer_in_out(transformer, inputCol, outputCol):
    transformer = transformer.copy()
    try:
        transformer.setInputCol(inputCol)
    except AttributeError:
        try:
            transformer.setInputCols([inputCol])
        except AttributeError:
            raise ValueError("Invalid transformer: " + str(transformer))

    try:
        transformer.setOutputCol(outputCol)
    except AttributeError:
        try:
            transformer.setOutputCols([outputCol])
        except AttributeError:
            # we accept transformers that do not have an outputCol
            # (as ColumnDropper)
            pass

    return transformer


class ColumnDropper(pyspark.ml.Transformer, pyspark.ml.feature.HasInputCol):
    def __init__(self, inputCol=None):
        super().__init__()
        self.setInputCol(inputCol)

    def transform(self, dataset):
        return dataset.drop(self.getInputCol())


class ColumnRenamer(pyspark.ml.Transformer,
                    pyspark.ml.feature.HasInputCol,
                    pyspark.ml.feature.HasOutputCol):
    def __init__(self, inputCol=None, outputCol=None):
        super().__init__()
        self.setInputCol(inputCol)
        self.setOutputCol(outputCol)

    def transform(self, dataset):
        return dataset.withColumnRenamed(self.getInputCol(), self.getOutputCol())


class FeatureBuilder(pyspark.ml.Transformer):
    def __init__(self, targetCol):
        super().__init__()
        self.targetCol = targetCol

    def transform(self, dataset):
        columns_to_regroup = [col for col in dataset.columns if col != self.targetCol]
        actual_transformer = VectorAssembler(inputCols=columns_to_regroup, outputCol='features')
        return actual_transformer.transform(dataset)


def map_by_dtypes(df_pipe, target_name, cat_transformers, num_transformers):
    """Maps the columns of a dataframe to specific transformations depending on their
    dtype.

    Categorical columns are taken through the cat_stransformers sequence
    (StringIndexer > OneHotEncoder for example) and numerical ones through the
    num_transformers sequence (VectorAssembler > StandardScaler for example).
    The target variable is set in the arguments and a StringIndexer is applied to it.
    The transformed features are then assembled by a VectorAssembler and a pyspark.ml
    pipeline object is returned.

    Parameters:

    df_pipe             a Spark dataframe to be transformed
    target_name         the name of the target column
    cat_transformers    list of pyspark.ml transformers for categorical columns
    num_transformers    list of pyspark.ml transformers for numerical columns

    TODO:
    Handle unseen labels.

    DONE:
    Make the user choose which transformations are to be applied for each dtype.
    Maybe pass dtypes as dictionaries as opposed to the string/not string dichotomy.

    Tested with the following transformers :
    import pipeasy_spark as ppz
    from pyspark.ml.feature import (
        OneHotEncoderEstimator,
        StringIndexer,
        VectorAssembler,
        StandardScaler,
    )
    ppz_pipeline= ppz.map_by_dtypes(df_pipe,
                                    target_name='Survived',
                                    cat_transformers=[StringIndexer, OneHotEncoderEstimator],
                                    num_transformers=[VectorAssembler, StandardScaler])

    """
    cat_columns = [item[0] for item in df_pipe.dtypes if item[1] == 'string']
    num_columns = [item[0] for item in df_pipe.dtypes if (not item[1] == 'string'
                                                          and not item[0] == target_name)]

    stages = []
    # Preparing categorical columns
    for cat_column in cat_columns:
        # Chain transformers
        cat_column_stages = []
        for idx, transformer in enumerate(cat_transformers):
            if idx == 0:
                transformer_args = dict(
                    inputCol=cat_column,
                    outputCol=cat_column + '_indexed'
                )
            else:
                transformer_args = dict(
                    inputCols=[cat_column_stages[idx - 1].getOutputCol()],
                    outputCols=[cat_column + '_transformed']
                )
            cat_column_stages += [transformer(**transformer_args)]

        # Add stages to main stages list
        stages += cat_column_stages

    # Preparing numerical columns
    for num_column in num_columns:
        # Chain transformers
        num_column_stages = []
        for idx, transformer in enumerate(num_transformers):
            if idx == 0:
                transformer_args = dict(
                    inputCols=[num_column],
                    outputCol=num_column + '_assembled')
            else:
                transformer_args = dict(
                    inputCol=num_column_stages[idx - 1].getOutputCol(),
                    outputCol=num_column + '_scaled')
            num_column_stages += [transformer(**transformer_args)]

        # Add stages to main stages list
        stages += num_column_stages

    # Preparing target variable
    label_indexer = StringIndexer(inputCol=target_name, outputCol='label')
    stages += [label_indexer]

    # Combine everything
    assembler_inputs = [c+'_transformed' for c in cat_columns] + [c+'_scaled' for c in num_columns]
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol='features')
    stages += [assembler]

    # Create a Pipeline
    pipeline = Pipeline(stages=stages)
    return pipeline
