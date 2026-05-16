"""PySpark adapter — local[*] mode inside the container. JVM stays warm across
all 11 ops because the runner runs them in one process. Lazy ops materialize
into noop sinks so we measure compute, not fetch."""

from contextlib import contextmanager

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import FILTER_THRESHOLD

VERSION = pyspark.__version__
IS_ASYNC = False


@contextmanager
def context():
    spark = (
        SparkSession.builder
        .appName("distributed-data-libs")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        yield spark
    finally:
        spark.stop()


def _spark():
    return SparkSession.getActiveSession()


def convert(path):
    spark = _spark()
    df = spark.read.parquet(path).cache()
    df.count()  # force materialization into the cache
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
