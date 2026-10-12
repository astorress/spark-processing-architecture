"""Adaptive Query Execution, shuffle partition coalescing, skew joins y broadcast."""
from pyspark.sql import functions as F

from lab.config import RAW
from lab.show import step, title
from lab.spark import session

title("04 - AQE, skew y joins")
spark = session("04-aqe-skew")
readings = spark.read.option("header", True).schema("meter_id string, ts timestamp, kwh double, voltage double").csv(
    f"file://{RAW}/readings")
meters = spark.read.option("header", True).option("inferSchema", True).csv(f"file://{RAW}/meters.csv")

step("Broadcast join: el catálogo cabe en memoria, Spark lo replica en los executors")
broadcast = readings.join(F.broadcast(meters), "meter_id")
broadcast.groupBy("city").agg(F.sum("kwh").alias("kwh")).show()
broadcast.explain(mode="formatted")

step("Sort-merge join: desactiva broadcast para ver el shuffle de ambas entradas")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
merge = readings.join(meters, "meter_id")
merge.groupBy("city").agg(F.sum("kwh").alias("kwh")).show()
merge.explain(mode="formatted")

step("AQE adapta el plan en runtime: coalesce shuffle partitions y mitiga particiones skewed")
print("AQE:", spark.conf.get("spark.sql.adaptive.enabled"))
print("coalesce:", spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))
print("skew join:", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))
print("En la Spark UI > SQL > Details compara el plan inicial con el plan adaptativo final.")
spark.stop()
print("Siguiente: python lessons/05_cache_checkpoint.py")
