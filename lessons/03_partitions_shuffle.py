"""Particiones, narrow/wide transformations, shuffle, repartition, coalesce y skew."""
from pyspark.sql import functions as F

from lab.config import RAW
from lab.show import step, table, title
from lab.spark import session

title("03 - Particiones y shuffle")
spark = session("03-partitions-shuffle")
sc = spark.sparkContext
lines = sc.textFile(f"file://{RAW}/readings/*.csv")

step("Una partición de entrada por split de archivos locales")
print("input partitions:", lines.getNumPartitions())

step("Map/filter son narrow: cada partición de salida depende de una partición de entrada")
narrow = lines.filter(lambda line: not line.startswith("meter_id")).map(lambda line: len(line))
print("narrow partition counts:", narrow.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()[:10])

step("groupByKey es wide: requiere shuffle; revisa el DAG y el número de particiones")
pairs = lines.filter(lambda line: not line.startswith("meter_id")).map(lambda line: (line.split(",")[0], 1))
grouped = pairs.reduceByKey(lambda a, b: a + b, numPartitions=8)
print(grouped.toDebugString().decode())
table([{"meter_id": k, "readings": n} for k, n in grouped.takeOrdered(8, key=lambda row: -row[1])])

step("repartition siempre baraja; coalesce puede reducir particiones evitando el shuffle")
df = spark.read.option("header", True).schema("meter_id string, ts timestamp, kwh double, voltage double").csv(
    f"file://{RAW}/readings")
print("current:", df.rdd.getNumPartitions(), "repartition(12):", df.repartition(12).rdd.getNumPartitions(),
      "coalesce(4):", df.coalesce(4).rdd.getNumPartitions())

step("Skew: la categoría industrial concentra pocos medidores pero mucho volumen de kWh")
meters = spark.read.option("header", True).option("inferSchema", True).csv(f"file://{RAW}/meters.csv")
by_type = df.join(meters, "meter_id").groupBy("customer_type").agg(F.count("*").alias("rows"), F.sum("kwh").alias("kwh"))
by_type.show()
spark.stop()
print("Siguiente: python lessons/04_aqe_skew.py")
