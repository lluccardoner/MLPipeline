import unittest

from pyspark.ml import *
from pyspark.sql import SparkSession

from stage_factory import StageFactory


class StageFactoryTest(unittest.TestCase):

    def setUp(self):
        # TODO spark as class variable
        self.spark = SparkSession \
            .builder \
            .appName("StageFactoryTest") \
            .getOrCreate()

    def test_get_stage_should_return_stage_from_ml_module(self):
        cls = Pipeline
        stage = StageFactory.get_stage('Pipeline')
        self.assertIsInstance(stage, cls, msg=f"Stage {stage} is not instance of {cls.__class__}")

        cls = feature.Tokenizer
        stage = StageFactory.get_stage('feature.Tokenizer')
        self.assertIsInstance(stage, cls,
                              msg=f"Stage {stage} is not instance of {cls.__class__}")

        cls = classification.LogisticRegression
        stage = StageFactory.get_stage('classification.LogisticRegression')
        self.assertIsInstance(stage, cls,
                              msg=f"Stage {stage} is not instance of {cls.__class__}")


if __name__ == '__main__':
    unittest.main()
