"""Driver, executors, RDD, transformaciones lazy, acciones, stages y tareas."""
import time

from lab.config import RAW
from lab.show import step, table, title
from lab.spark import describe, executor_memory_status, session

title("01 - Modelo de ejecución")
spark = session("01-execution-model")
sc = spark.sparkContext
describe(spark)

step("Master y workers: el driver pide recursos al master y los ejecutores procesan particiones")
table(executor_memory_status(spark))

step("textFile define una lectura distribuida; aún no ha leído el CSV")
lines = sc.textFile(f"file://{RAW}/readings/*.csv")
print("partitions:", lines.getNumPartitions())

step("map/filter/reduceByKey construyen un DAG en el driver: transformaciones lazy")
started = time.perf_counter()
daily = (lines.filter(lambda row: not row.startswith("meter_id"))
         .map(lambda row: row.split(","))
         .map(lambda row: (row[1][:10], float(row[2])))
         .reduceByKey(lambda left, right: left + right))
print(f"plan listo en {(time.perf_counter() - started) * 1000:.1f} ms; no action, no job")
print(daily.toDebugString().decode())

step("collect es una acción: ejecuta stages y trae solo el pequeño agregado al driver")
started = time.perf_counter()
table([{"day": day, "kwh": round(kwh, 1)} for day, kwh in sorted(daily.collect())[:5]])
print("seconds:", round(time.perf_counter() - started, 2))
print("Una tarea procesa una partición. El shuffle de reduceByKey crea el límite entre stages.")
spark.stop()
print("Siguiente: python lessons/02_spark_sql_catalyst.py")
