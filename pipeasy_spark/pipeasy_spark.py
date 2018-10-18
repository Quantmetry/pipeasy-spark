# -*- coding: utf-8 -*-

"""Main module."""
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, StandardScaler

'''
The pipeasy-spark package provides a set of convenience classes and functions that
make it easier to map each column of a Spark dataframe (or subsets of columns) to
user-specified transformations. Increasingly complex features are provided:

map_by_dtypes()     allows a simple mapping of features to user-specified transformations
                    according to their dtypes. Each dtype is assigned the sequence of
                    transformations passed in the arguments as dictionaries.

map_by_column()     allows mapping transformations at a more detailed level. Each column
                    of the dataframe (or subset thereof) can be assigned a specific
                    sequence of transformations.

Each function returns a Spark dataframe that is ready to be used by Spark.ml
'''

def map_by_dtypes(df_pipe, target_name):
    '''
    Maps the columns of a dataframe to specific transformations depending on their
    dtype.

    Categorical columns are taken through a StringIndexer > OneHotEncoder
    sequence and numerical ones through a VectorAssembler > StandardScaler sequence.
    The target variable is set in the arguments and a StringIndexer is applied to it.
    The transformed features are then assembled by a VectorAssembler and a transformed
    dataframe is returned, ready to be used in spark.ml

    Parameters:

    df_pipe         a Spark dataframe to be transformed
    target_name     the name of the target column

    TODO:
    Make the user choose which transformations are to be applied for each dtype.
    Maybe pass dtypes as dictionaries as opposed to the string/not string dichotomy.
    '''
    cat_columns = [item[0] for item in df_pipe.dtypes if item[1]=='string']
    num_columns = [item[0] for item in df_pipe.dtypes if (not item[1]=='string'
                                                          and not item[0]==target_name)]

    stages = []
    # Preparing categorical columns
    for cat_column in cat_columns:
        # Catgorical indexing
        stringIndexer = StringIndexer(inputCol=cat_column,
                                      outputCol=cat_column+'Index')
        # One-hot encoding
        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()],
                                         outputCols=[cat_column+'ClassVec'])
        # Add stages
        stages += [stringIndexer, encoder]

    # Preparing numerical columns
    for num_column in num_columns:
        #vector assembler
        num_assembler = VectorAssembler(inputCols=[num_column],
                                        outputCol=num_column+'Assembled')
        # scaling
        scaler = StandardScaler(inputCol=num_assembler.getOutputCol(),
                                outputCol=num_column+'Scaled')
        # add stages
        stages += [num_assembler, scaler]

    # Preparing target variable
    labelIndexer = StringIndexer(inputCol=target_name, outputCol='label')
    stages += [labelIndexer]

    # Combine everything
    assemblerInputs = [c+'ClassVec' for c in cat_columns] + [c+'Scaled' for c in num_columns]
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol='features')
    stages += [assembler]

    # Create a Pipeline
    pipeline = Pipeline(stages=stages)
    df_pipe_out = pipeline.fit(df_pipe).transform(df_pipe)
    return df_pipe_out
