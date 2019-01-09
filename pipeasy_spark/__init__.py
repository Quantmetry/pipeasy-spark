# -*- coding: utf-8 -*-

"""Top-level package for pipeasy-spark."""

__author__ = """Benjamin Habert"""
__email__ = 'bhabert@quantmetry.com'
__version__ = '0.1.2'

from .core import map_by_column
from .convenience import map_by_dtypes


__all__ = [
    'map_by_column',
    'map_by_dtypes',
]
