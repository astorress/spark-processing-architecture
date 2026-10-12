"""Persistencia, cache, storage levels, checkpoint y lineage truncation."""
import time

from pyspark import StorageLevel

from lab.config import RAW
from lab.show import step, title
from lab.spark import session

title("05 - Cache y checkpoint")
spark = session("05-cache-checkpoint")
sc = spark.sparkContext
source = sc.textFile(f"file://{RAW}/readings/*.csv").filter(lambda row: not row.startswith("meter_id"))
parsed = source.map(lambda row: (row.split(",")[0], float(row.split(",")[2])))

step("Sin persistencia: dos acciones vuelven a leer y parsear el origen")
for i in (1, 2):
    started = time.perf_counter()
    print(f"run {i}: {parsed.count():,} rows · {time.perf_counter() - started:.2f}s")

step("MEMORY_AND_DISK: conserva particiones en RAM y derrama a disco si no caben")
parsed.persist(StorageLevel.MEMORY_AND_DISK)
started = time.perf_counter()
parsed.count()
print(f"materialize: {time.perf_counter() - started:.2f}s")
started = time.perf_counter()
parsed.count()
print(f"cached action: {time.perf_counter() - started:.2f}s")
print("Revisa Storage en Spark UI para bytes y particiones persistidos.")

step("Checkpoint: guarda resultados confiables en disco y corta el lineage largo")
sc.setCheckpointDir("file:///app/data/checkpoints")
checkpointed = parsed.filter(lambda pair: pair[1] > 0)
checkpointed.checkpoint()
print("checkpoint rows:", checkpointed.count())
print(checkpointed.toDebugString().decode())
parsed.unpersist()
spark.stop()
print("Siguiente: python lessons/06_delta_lake.py")
