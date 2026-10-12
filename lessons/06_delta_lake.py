"""Delta Lake: transaction log, ACID writes, schema enforcement, MERGE y time travel."""
from pathlib import Path
from datetime import date, datetime

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from lab.config import BRONZE, RAW
from lab.show import step, title
from lab.config import session

title("06 - Delta Lake")
spark = session("06-delta-lake", delta=True)
input_path = f"file://{RAW}/readings"
delta_path = f"file://{BRONZE}/readings_delta"
schema = "meter_id string, ts timestamp, kwh double, voltage double"
source = spark.read.option("header", True).schema(schema).csv(input_path)

step("Escritura Delta: parquet data files + _delta_log con commits atómicos")
(source.withColumn("date", F.to_date("ts")).write.format("delta").mode("overwrite").partitionBy("date")
 .save(delta_path))
print(DeltaTable.forPath(spark, delta_path).history().select("version", "operation", "operationMetrics").show(5, False))

step("Lectura y time travel: versionAsOf reconstruye una vista consistente")
latest = spark.read.format("delta").load(delta_path)
print("latest rows:", latest.count())
old = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
print("version 0 rows:", old.count())

step("Schema enforcement: un write incompatible se rechaza en vez de corromper la tabla")
try:
    spark.createDataFrame([("M99999", "campo-extra")], "meter_id string, extra string").write.format("delta").mode(
        "append").save(delta_path)
except Exception as error:
    print(type(error).__name__, str(error).splitlines()[0][:180])

step("MERGE upsert: insertar o actualizar un registro de corrección")
correction_schema = f"{schema}, date date"
correction = spark.createDataFrame([("M00001", datetime(2026, 9, 1), 9.99, 120.0, date(2026, 9, 1))],
                                   correction_schema)
DeltaTable.forPath(spark, delta_path).alias("target").merge(
    correction.alias("source"), "target.meter_id = source.meter_id AND target.ts = source.ts").whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll().execute()
spark.read.format("delta").load(delta_path).where("meter_id='M00001' AND ts='2026-09-01 00:00:00'").show()
spark.stop()
print("Siguiente: python lessons/07_structured_streaming.py")
