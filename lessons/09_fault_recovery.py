"""Lineage y recuperación: Spark reintenta la partición que falla, no todo el job."""
from pathlib import Path

from lab.config import session
from lab.show import step, title

title("09 - Recuperación por lineage")
spark = session("09-fault-recovery", **{"spark.task.maxFailures": "4"})
sc = spark.sparkContext
source = sc.parallelize(range(12), 4)
markers = Path("/app/data/retry-markers")
markers.mkdir(parents=True, exist_ok=True)
for marker in markers.glob("partition-*.done"):
    marker.unlink()


def flaky_partition(index, values):
    values = list(values)
    marker = markers / f"partition-{index}.done"
    if index == 2 and not marker.exists():
        marker.write_text("failed once")
        raise RuntimeError("simulated transient executor-task failure")
    return [(index, len(values), sum(values))]


step("La partición 2 falla una vez; el scheduler reintenta la task")
result = source.mapPartitionsWithIndex(flaky_partition).collect()
print("task results:", sorted(result))

step("El lineage sigue siendo la fuente de verdad; Spark recalcula solo lo que perdió")
print(source.map(lambda value: value * 2).toDebugString().decode())
print("Abre la Spark UI > Stages y observa failed task / task attempt. Las otras particiones no se repiten.")
print("Las transformaciones deben ser deterministas o idempotentes: Spark puede ejecutarlas más de una vez.")
spark.stop()
print("Siguiente: python lessons/10_history_server.py")
