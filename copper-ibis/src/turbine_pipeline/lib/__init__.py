"""Pure PySpark transforms.

Nothing in this package imports ``pyspark.pipelines``. Every function here takes and returns
a DataFrame, so the whole package runs under a plain local SparkSession in ``tests/unit``.
The ``transformations`` package holds the thin decorated wrappers that call into these.
"""
