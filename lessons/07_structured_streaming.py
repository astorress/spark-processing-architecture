"""Structured Streaming: micro-batches, event time, watermark, state store y checkpoints."""
from pyspark.sql import functions as F

from lab.config import DATA, session
from lab.show import step, title

title("07 - Structured Streaming")
spark = session("07-structured-streaming")

step("Rate source: genera eventos distribuidos con timestamp; cada trigger procesa un micro-batch")
stream = (spark.readStream.format("rate").option("rowsPerSecond", 20).option("numPartitions", 2).load()
          .select(F.col("timestamp").alias("event_time"), (F.col("value") % 8).alias("meter_group"),
                  (F.col("value") % 100 / 10).alias("kwh")))

step("Watermark y ventana: el estado de agregación se conserva hasta que llega el watermark")
windowed = (stream.withWatermark("event_time", "10 seconds")
            .groupBy(F.window("event_time", "10 seconds"), "meter_group")
            .agg(F.round(F.sum("kwh"), 2).alias("kwh"), F.count("*").alias("events")))
checkpoint = f"file://{DATA}/checkpoints/streaming"
query = (windowed.writeStream.format("console").outputMode("append").option("truncate", False)
         .option("checkpointLocation", checkpoint).trigger(processingTime="5 seconds").start())
query.awaitTermination(20)
query.stop()

step("Progreso: inputRowsPerSecond, processedRowsPerSecond y bytes de estado")
for progress in query.recentProgress[-4:]:
    print({key: progress.get(key) for key in ("batchId", "numInputRows", "inputRowsPerSecond", "processedRowsPerSecond")})
print("El checkpoint guarda offsets, commits y estado para reanudar la consulta. No lo reutilices con un plan distinto.")
spark.stop()
print("Siguiente: python lessons/08_mllib_pipeline.py")
