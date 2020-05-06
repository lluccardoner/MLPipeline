from pyspark.sql import SparkSession

from config_factory import ConfigFactory
from src.step_factory import StepFactory

# Example from https://spark.apache.org/docs/latest/ml-pipeline.html#example-pipeline
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("MLPipeline") \
        .getOrCreate()

    step_config = ConfigFactory.create_config("config/pipeline_config.json")
    prediction = StepFactory.create_step(spark, step_config).execute()

    selected = prediction.select("id", "text", "probability", "prediction")
    for row in selected.collect():
        rid, text, prob, prediction = row
        print("(%d, %s) --> prob=%s, prediction=%f" % (rid, text, str(prob), prediction))

    spark.stop()
