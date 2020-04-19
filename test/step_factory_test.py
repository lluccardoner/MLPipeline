import unittest

from pyspark.ml import *
from pyspark.sql import SparkSession

from step_factory import StepFactory


class StepFactoryTest(unittest.TestCase):
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

    def test_create_step_should_create_stage_of_step(self):
        my_stage = {
            "name": "feature.Tokenizer",
            "params": {
                "inputCol": "text",
                "outputCol": "words"
            }
        }

        my_fit_step = {
            "name": "fit",
            "params": {},
            "stage": my_stage
        }

        step = StepFactory.create_step(my_fit_step)

        cls = feature.Tokenizer
        self.assertIsInstance(step.stage, cls,
                              msg=f"Stage {step.stage} of step {step} is not instance of {cls.__class__}")

    def test_create_step_should_execute_previous_step(self):
        training = self.spark.createDataFrame([
            (0, "a b c d e spark", 1.0),
            (1, "b d", 0.0),
            (2, "spark f g h", 1.0),
            (3, "hadoop mapreduce", 0.0)
        ], ["id", "text", "label"])

        my_stage = {
            "name": "Pipeline",
            "params": {
                "stages": [
                    {
                        "name": "feature.Tokenizer",
                        "params": {
                            "inputCol": "text",
                            "outputCol": "words"
                        }
                    },
                    {
                        "name": "feature.HashingTF",
                        "params": {
                            "inputCol": "words",
                            "outputCol": "features"
                        }
                    },
                    {
                        "name": "classification.LogisticRegression",
                        "params": {
                            "maxIter": 10,
                            "regParam": 0.001
                        }
                    }
                ]
            }
        }

        my_fit_step = {
            "name": "fit",
            "params": {
                "dataset": training
            },
            "stage": my_stage
        }

        my_predict_step = {
            "name": "transform",
            "params": {},
            "stage": my_fit_step
        }

        step = StepFactory.create_step(my_predict_step)

        cls = classification.LogisticRegressionModel
        self.assertIsInstance(step.stage, cls,
                              msg=f"Stage {step.stage} of step {step} is not instance of {cls.__class__}")


if __name__ == '__main__':
    unittest.main()
