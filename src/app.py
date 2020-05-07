import sys

from pyspark.sql import SparkSession

from config_factory import ConfigFactory
from step_factory import StepFactory

if __name__ == "__main__":

    if len(sys.argv) == 1:
        print("No config file given")
        exit(0)

    config_path = sys.argv[1]

    spark = SparkSession \
        .builder \
        .appName("MLPipeline") \
        .getOrCreate()

    print("Loading config from: " + config_path)
    step_config = ConfigFactory.create_config(config_path)
    print("Executing step...")
    StepFactory.create_step(spark, step_config).execute()
