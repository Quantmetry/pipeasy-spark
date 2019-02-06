=======
History
=======

0.2.0 (2019-01-06)
------------------

First usable version of the package. We decided on the api:

- ``pipeasy_spark.build_pipeline(column_transformers={'column': []})`` is the core function 
  where you can define a list of transormers for each columns.
- ``pipeasy_spark.build_pipeline_by_dtypes(df, string_transformers=[])`` allows
  you to define a list of transormers for two types of columns: ``string_`` and ``numeric_``.
- ``pipeasy_spark.build_default_pipeline(df, exclude_columns=['target'])`` builds a default
  transformer for the ``df`` dataframe.

0.1.2 (2018-10-12)
------------------

* I am still learning how all these tools interact with each other

0.1.1 (2018-10-12)
------------------

* First release on PyPI.
