# -*- coding: utf-8 -*-

"""Main module."""
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, StandardScaler

def map_by_dtypes(df_pipe, target_name):
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
