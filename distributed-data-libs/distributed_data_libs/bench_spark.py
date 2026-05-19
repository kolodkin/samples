"""PySpark adapter — Spark Connect thin client → spark-server JVM sidecar."""

import os
from contextlib import contextmanager

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import FILTER_THRESHOLD, SAMPLE_LIMIT, SAMPLE_OFFSET

VERSION = pyspark.__version__
IS_ASYNC = False

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
    df.count()  # force server-side cache materialization
    return df


def _materialize(df):
    df.write.format("noop").mode("overwrite").save()


def _sample(df):
    return df.offset(SAMPLE_OFFSET).limit(SAMPLE_LIMIT).collect()


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
    "Column multiply page": lambda df: _sample(df.select((F.col("amount") * F.col("quantity")).alias("p"))),
    "Filter rows page":     lambda df: _sample(df.filter(F.col("amount") > FILTER_THRESHOLD)),
    "Sort page":            lambda df: _sample(df.orderBy(F.col("amount").desc())),
}
