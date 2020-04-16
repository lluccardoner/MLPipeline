import unittest

from pyspark.ml import *
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

from step import Step


class StepTest(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession \
            .builder \
            .appName("StageFactoryTest") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_execute_should_fit_stage(self):
        cls = Model

        dataset = self.spark.createDataFrame([
            (0, Vectors.dense([1,2]), 1),
            (1, Vectors.dense([1,3]), 1),
            (2, Vectors.dense([2,3]), 0),
            (3, Vectors.dense([4,5]), 1)
        ], ["id", "features", "label"])

        params = {
            "dataset": dataset
        }

        step = Step('fit', params)
        stage = classification.LogisticRegression()

        model = step.execute(stage)

        self.assertIsInstance(model, cls, msg=f"Result {model} is not instance of {cls.__class__}")


if __name__ == '__main__':
    unittest.main()
