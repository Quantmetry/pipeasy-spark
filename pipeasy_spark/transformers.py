import pyspark.ml
import pyspark.ml.feature


def set_transformer_in_out(transformer, inputCol, outputCol):
    transformer = transformer.copy()
    try:
        transformer.setInputCol(inputCol)
    except AttributeError:
        try:
            transformer.setInputCols([inputCol])
        except AttributeError:
            raise ValueError("Invalid transformer: " + str(transformer.__class__))

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


class ColumnDropper(pyspark.ml.Transformer, pyspark.ml.feature.HasInputCols):
    def __init__(self, inputCols=None):
        super().__init__()
        self.setInputCols(inputCols)

    def transform(self, dataset):
        return dataset.drop(*self.getInputCols())


class ColumnRenamer(pyspark.ml.Transformer,
                    pyspark.ml.feature.HasInputCol,
                    pyspark.ml.feature.HasOutputCol):
    def __init__(self, inputCol=None, outputCol=None):
        super().__init__()
        self.setInputCol(inputCol)
        self.setOutputCol(outputCol)

    def transform(self, dataset):
        return dataset.withColumnRenamed(self.getInputCol(), self.getOutputCol())
