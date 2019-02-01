# -*- coding: utf-8 -*-

"""Top-level package for pipeasy-spark.

The pipeasy-spark package provides a set of convenience functions that
make it easier to map each column of a Spark dataframe (or subsets of columns) to
user-specified transformations.

Each of the following functions returns a pyspark.ml.Pipeline object:

build_pipeline()    allows mapping transformations at a more detailed level. Each column
                    of the dataframe (or subset thereof) can be assigned a specific
                    sequence of transformations.

"""

__author__ = """Quantmetry"""
__email__ = 'bhabert@quantmetry.com'
__version__ = '0.1.2'

from .core import build_pipeline
from .convenience import map_by_dtypes


__all__ = [
    'build_pipeline',
    'map_by_dtypes',
]
