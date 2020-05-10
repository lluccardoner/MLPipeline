import os
import unittest

from pyspark.ml import *
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

from ml_pipeline.step import Step


class StepTest(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession \
            .builder \
            .appName("MLPipeline") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_execute_should_fit_stage(self):
        cls = Model

        dataset = self.spark.createDataFrame([
            (0, Vectors.dense([1, 2]), 1),
            (1, Vectors.dense([1, 3]), 1),
            (2, Vectors.dense([2, 3]), 0),
            (3, Vectors.dense([4, 5]), 1)
        ], ["id", "features", "label"])

        params = {
            "dataset": dataset
        }

        stage = classification.LogisticRegression()
        step = Step('fit', params, stage)

        model = step.execute()

        self.assertIsInstance(model, cls, msg=f"Result {model} is not instance of {cls.__class__}")

    def test_execute_should_load_stage(self):
        params = {
            "path": "resources/stages/LogisticRegression"
        }

        stage = classification.LogisticRegression()
        step = Step('load', params, stage)

        loaded = step.execute()

        cls = classification.LogisticRegression
        self.assertIsInstance(loaded, cls, msg=f"Loaded stage {loaded} is not instance of {cls.__class__}")

    def test_execute_should_save_stage(self):
        output = "resources/stages/Tokenizer"
        # TODO remove output

        params = {
            "path": output,
            "overwrite": "true"
        }

        stage = feature.Tokenizer(inputCol="text", outputCol="words")
        step = Step('save', params, stage)

        step.execute()
        self.assertTrue(os.path.exists(output))


if __name__ == '__main__':
    unittest.main()
