# -*- coding: utf-8 -*-

"""Top-level package for pipeasy-spark.

The pipeasy-spark package provides a set of convenience functions that
make it easier to map each column of a Spark dataframe (or subsets of columns) to
user-specified transformations.
"""

__author__ = """Quantmetry"""
__email__ = 'bhabert@quantmetry.com'
__version__ = '0.2.1'

from .core import build_pipeline
from .convenience import (
    build_pipeline_by_dtypes,
    build_default_pipeline,
)


__all__ = [
    'build_pipeline',
    'build_pipeline_by_dtypes',
    'build_default_pipeline',
]
