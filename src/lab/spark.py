from pyspark.sql import SparkSession

from lab.config import MASTER, session


def describe(spark: SparkSession):
    sc = spark.sparkContext
    print(f"Spark {spark.version} · master={sc.master} · app={sc.applicationId} · defaultParallelism={sc.defaultParallelism}")
    print(f"workers UI: http://localhost:8081 · aplicación UI: http://localhost:{spark.conf.get('spark.ui.port', '4041')}")


def executor_hosts(spark: SparkSession):
    return sorted(row["host:port"].split(":")[0] for row in executor_memory_status(spark)
                  if not row["host:port"].startswith("spark-lab"))


def executor_memory_status(spark: SparkSession):
    """Read Spark's Scala Map using its iterator, stable across Scala collection wrappers."""
    statuses = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
    entries = statuses.iterator()
    rows = []
    while entries.hasNext():
        entry = entries.next()
        memory = entry._2()
        rows.append({"host:port": entry._1(), "memory_bytes": int(memory._1()), "free_bytes": int(memory._2())})
    return rows
