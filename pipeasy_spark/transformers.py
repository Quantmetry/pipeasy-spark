import pyspark


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
        actual_transformer = pyspark.ml.feature.VectorAssembler(inputCols=columns_to_regroup,
                                                                outputCol='features')
        return actual_transformer.transform(dataset)
