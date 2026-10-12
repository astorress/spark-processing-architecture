"""MLlib: Pipeline, temporal split, GBT regression y métricas en holdout futuro."""
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import functions as F

from lab.config import GOLD, RAW, session
from lab.show import step, table, title

title("08 - MLlib Pipeline")
spark = session("08-mllib-pipeline")
readings = spark.read.option("header", True).schema("meter_id string, ts timestamp, kwh double, voltage double").csv(
    f"file://{RAW}/readings")
meters = spark.read.option("header", True).option("inferSchema", True).csv(f"file://{RAW}/meters.csv")
hourly = (readings.withColumn("hour", F.date_trunc("hour", "ts")).groupBy("meter_id", "hour")
          .agg(F.sum("kwh").alias("label"), F.avg("voltage").alias("voltage"))
          .join(meters.select("meter_id", "city", "customer_type", "contracted_kw"), "meter_id")
          .withColumn("hour_of_day", F.hour("hour")).withColumn("day_of_week", F.dayofweek("hour"))
          .withColumn("day", F.to_date("hour")))

step("Split temporal: entrenar 1-23 septiembre, evaluar 24-30 septiembre")
train, test = hourly.where("day < '2026-09-24'"), hourly.where("day >= '2026-09-24'")
print("train:", train.count(), "test:", test.count())

step("Pipeline: categorías -> vector de features -> Gradient Boosted Trees")
pipeline = Pipeline(stages=[
    StringIndexer(inputCols=["city", "customer_type"], outputCols=["city_idx", "type_idx"], handleInvalid="keep"),
    VectorAssembler(inputCols=["city_idx", "type_idx", "contracted_kw", "hour_of_day", "day_of_week", "voltage"],
                    outputCol="features"),
    GBTRegressor(featuresCol="features", labelCol="label", maxIter=12, maxDepth=4, seed=42),
])
model = pipeline.fit(train)
predictions = model.transform(test).cache()

step("Métricas holdout: RMSE y R2")
rows = []
for metric in ("rmse", "r2"):
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName=metric)
    rows.append({"metric": metric, "value": round(evaluator.evaluate(predictions), 4)})
table(rows)
predictions.select("meter_id", "hour", "label", "prediction").show(10, False)

step("Persistir el PipelineModel para batch scoring futuro")
model_path = f"file://{GOLD}/hourly_model"
model.write().overwrite().save(model_path)
print("saved:", model_path)
predictions.unpersist()
spark.stop()
print("Siguiente: python lessons/09_fault_recovery.py")
