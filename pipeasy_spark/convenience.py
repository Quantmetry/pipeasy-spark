from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoderEstimator,
    VectorAssembler,
    StandardScaler,
)
from pyspark.sql.types import (
    NumericType,
    StringType,
)

from .core import build_pipeline


def build_pipeline_by_dtypes(dataframe,
                             exclude_columns=tuple(),
                             string_transformers=tuple(),
                             numeric_transformers=tuple(),
                             ):
    """Build simple transformation pipeline (untrained) for the given dataframe.

    Parameters
    ----------
    dataframe: pyspark.sql.Dataframe
        only the schema of the dataframe is used, not actual data.
    exclude_columns: list of str
        name of columns for which we want no transformation to apply.
    string_transformers: list of transformer instances
        The successive transformations that will be applied to string columns
        Each element is an instance of a pyspark.ml.feature transformer class.
    numeric_transformers: list of transformer instances
        The successive transformations that will be applied to numeric columns
        Each element is an instance of a pyspark.ml.feature transformer class.

    Returns
    -------
    pipeline: pyspark.ml.Pipeline instance (untrained)

    """
    column_transformers = {}

    for field in dataframe.schema:
        column, dtype = field.name, field.dataType
        if column not in exclude_columns:
            if isinstance(dtype, NumericType):
                column_transformers[column] = numeric_transformers
            elif isinstance(dtype, StringType):
                column_transformers[column] = string_transformers

    return build_pipeline(column_transformers)


def build_default_pipeline(dataframe, exclude_columns=tuple()):
    """Build simple transformation pipeline (untrained) for the given dataframe.

    By defaults numeric columns are processed with StandardScaler and string columns
    are processed with StringIndexer + OneHotEncoderEstimator

    Parameters
    ----------
        dataframe: pyspark.sql.Dataframe
            only the schema of the dataframe is used, not actual data.
        exclude_columns: list of str
            name of columns for which we want no transformation to apply.

    Returns
    -------
    pipeline: pyspark.ml.Pipeline instance (untrained)

    """
    return build_pipeline_by_dtypes(
        dataframe,
        exclude_columns=exclude_columns,
        string_transformers=(StringIndexer(), OneHotEncoderEstimator()),
        numeric_transformers=(VectorAssembler(), StandardScaler())
    )
