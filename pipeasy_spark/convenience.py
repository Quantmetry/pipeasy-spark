"""
map_by_dtypes()     allows a simple mapping of features to user-specified transformations
                    according to their dtypes. Each dtype is assigned the sequence of
                    transformations passed in the arguments as dictionaries.
"""
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    VectorAssembler,
)


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
