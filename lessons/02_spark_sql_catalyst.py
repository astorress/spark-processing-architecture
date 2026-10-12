"""DataFrames, Catalyst, predicate/column pruning, physical plans y SQL."""
from pyspark.sql import functions as F

from lab.config import RAW
from lab.show import step, title
from lab.spark import session

title("02 - Spark SQL y Catalyst")
spark = session("02-spark-sql")
schema = "meter_id string, ts timestamp, kwh double, voltage double"
readings = spark.read.option("header", True).schema(schema).csv(f"file://{RAW}/readings")
meters = spark.read.option("header", True).option("inferSchema", True).csv(f"file://{RAW}/meters.csv")
readings.createOrReplaceTempView("readings")
meters.createOrReplaceTempView("meters")

step("Schema explícito: inferSchema hace un recorrido extra para adivinar tipos")
readings.printSchema()

step("Consulta SQL: agregado mensual por ciudad y tipo de cliente")
query = spark.sql("""
    SELECT m.city, m.customer_type, count(DISTINCT m.meter_id) AS meters,
           round(sum(r.kwh) / 1000, 1) AS mwh
    FROM readings r JOIN meters m USING (meter_id)
    WHERE r.voltage IS NOT NULL AND r.kwh >= 0
    GROUP BY m.city, m.customer_type
    ORDER BY mwh DESC
""")
query.show(12, truncate=False)

step("Catalyst: el plan optimizado empuja filtros y columnas al lector")
query.explain(mode="formatted")
print("Busca ReadSchema, PushedFilters, Exchange y Sort. Exchange implica movimiento de datos (shuffle).")

step("SQL equivalente: DataFrame API describe el mismo plan lógico")
(readings.where(F.col("kwh") >= 0).groupBy(F.to_date("ts").alias("day")).agg(F.sum("kwh").alias("kwh"))
 .orderBy("day").show(5))
spark.stop()
print("Siguiente: python lessons/03_partitions_shuffle.py")
