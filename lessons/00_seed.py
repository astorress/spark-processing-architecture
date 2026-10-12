"""Genera el mismo dataset AMI que los laboratorios Hadoop y Kafka."""
import csv
from pathlib import Path

from lab import dataset
from lab.config import RAW
from lab.show import step, title

root = Path(RAW)
title("00 - Dataset AMI")
root.mkdir(parents=True, exist_ok=True)

step("Catálogo de medidores")
with (root / "meters.csv").open("w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=dataset.METER_COLUMNS)
    writer.writeheader()
    writer.writerows(dataset.meters())
print("medidores:", len(dataset.METER_LIST))

step("Lecturas CSV por día: 2,88 millones de filas en bronze")
readings = root / "readings"
readings.mkdir(exist_ok=True)
for day in dataset.days():
    (readings / f"readings_{day}.csv").write_text(dataset.readings_csv(day))
print("archivos:", len(list(readings.glob("*.csv"))))
print("tamaño MB:", round(sum(p.stat().st_size for p in readings.glob("*.csv")) / 2**20, 1))
print("Próxima: python lessons/01_execution_model.py")
