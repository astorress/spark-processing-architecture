import os

from pyspark.sql import SparkSession

MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
DATA = "/app/data"
RAW = f"{DATA}/raw"
BRONZE = f"{DATA}/lake/bronze"
SILVER = f"{DATA}/lake/silver"
GOLD = f"{DATA}/lake/gold"
EVENTLOG = f"{DATA}/eventlog"


def session(name, delta=False, **configs):
    builder = SparkSession.builder.master(MASTER).appName(name)
    for key, value in configs.items():
        builder = builder.config(key, value)
    if delta:
        from delta import configure_spark_with_delta_pip

        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        builder = configure_spark_with_delta_pip(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
