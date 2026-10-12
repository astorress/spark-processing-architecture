"""Event logs, History Server y configuración reproducible."""
from pathlib import Path

from lab.config import EVENTLOG, session
from lab.show import step, table, title

title("10 - Spark History Server")
spark = session("10-history-server")
sc = spark.sparkContext

step("Configuración efectiva de una aplicación")
keys = ("spark.master", "spark.app.name", "spark.executor.cores", "spark.executor.memory", "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled", "spark.eventLog.enabled", "spark.eventLog.dir")
table([{"key": key, "value": spark.conf.get(key, "(SparkConf / default)")} for key in keys])

step("Un action deja evidencia en el event log")
sc.parallelize(range(1_000_000), 8).map(lambda value: value * value).sum()
spark.stop()
files = list(Path(EVENTLOG).glob("*")) if Path(EVENTLOG).exists() else []
print("event log files:", len(files))
print("History Server: http://localhost:18080 (revisa jobs, stages, executors y SQL después de terminar).")
print("La UI viva es efímera; los event logs permiten diagnosticar aplicaciones terminadas.")
print("\nFin de las lecciones. Prueba el sandbox en notebooks/sandbox.ipynb")
