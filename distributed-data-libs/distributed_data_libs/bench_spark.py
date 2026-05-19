"""PySpark adapter — client-server topology via Spark Connect. The thin
Python client (this container) sends DataFrame plans over gRPC to the
spark-server sidecar (JVM); the server reads parquet, runs all compute,
and streams results back as Arrow. Lazy ops materialize into noop sinks
so we measure server-side compute, not driver-side fetch."""

import os
from contextlib import contextmanager

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import FILTER_THRESHOLD

VERSION = pyspark.__version__
IS_ASYNC = False

# Connect server endpoint. Defaults to the compose service name; override
# with SPARK_REMOTE for standalone runs.
_REMOTE = os.environ.get("SPARK_REMOTE", "sc://spark-server:15002")


@contextmanager
def context():
    spark = (
        SparkSession.builder
        .appName("distributed-data-libs")
        .remote(_REMOTE)
        .getOrCreate()
    )
    try:
        yield spark
    finally:
        spark.stop()


def _spark():
    return SparkSession.getActiveSession()


def convert(path):
    spark = _spark()
    df = spark.read.parquet(path).cache()
    df.count()  # force materialization into the server-side cache
    return df


def _materialize(df):
    df.write.format("noop").mode("overwrite").save()


BENCHMARKS = {
    "Column sum":         lambda df: df.agg(F.sum("amount")).collect(),
    "Column multiply":    lambda df: _materialize(df.select((F.col("amount") * F.col("quantity")).alias("p"))),
    "Filter rows":        lambda df: _materialize(df.filter(F.col("amount") > FILTER_THRESHOLD)),
    "Sort":               lambda df: _materialize(df.orderBy(F.col("amount").desc())),
    "Count distinct":     lambda df: df.agg(F.countDistinct("category")).collect(),
    "Group-by sum":       lambda df: df.groupBy("category").agg(F.sum("amount")).collect(),
    "Group-by count":     lambda df: df.groupBy("category").count().collect(),
    "Group-by multi-agg": lambda df: df.groupBy("category").agg(
        F.sum("amount"), F.mean("amount"), F.min("amount"), F.max("amount")
    ).collect(),
    "Multi-key group-by": lambda df: df.groupBy("category", "subcategory").agg(F.sum("amount")).collect(),
    "High-card group-by": lambda df: df.groupBy("subcategory").agg(F.sum("amount")).collect(),
}
