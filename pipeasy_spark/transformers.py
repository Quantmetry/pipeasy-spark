import pyspark.ml
import pyspark.ml.feature


def set_transformer_in_out(transformer, inputCol, outputCol):
    """Set input and output column(s) of a transformer instance."""
    transformer = transformer.copy()
    try:
        transformer.setInputCol(inputCol)
    except AttributeError:
        try:
            transformer.setInputCols([inputCol])
        except AttributeError:
            message = (
                "Invalid transformer (doesn't have setInputCol or setInputCols): ",
                str(transformer.__class__)
            )
            raise ValueError(message)

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
    """Transformer to drop several columns from a dataset."""
    def __init__(self, inputCols=None):
        super().__init__()
        self.setInputCols(inputCols)

    def transform(self, dataset):
        return dataset.drop(*self.getInputCols())


class ColumnRenamer(pyspark.ml.Transformer,
                    pyspark.ml.feature.HasInputCol,
                    pyspark.ml.feature.HasOutputCol):
    """Transformer to rename a column to another."""
    def __init__(self, inputCol=None, outputCol=None):
        super().__init__()
        self.setInputCol(inputCol)
        self.setOutputCol(outputCol)

    def transform(self, dataset):
        return dataset.withColumnRenamed(self.getInputCol(), self.getOutputCol())
