import unittest

from pyspark.ml import *
from pyspark.sql import SparkSession

from stage_factory import StageFactory


class StageFactoryTest(unittest.TestCase):
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

    def test_get_stage_should_throw_error_if_wrong_name(self):
        with self.assertRaises(NameError):
            StageFactory.get_stage('module.NoStage')

    def test_set_params_should_set_stage_params_if_given(self):
        stage = feature.HashingTF()
        params = {
            "inputCol": "words",
            "outputCol": "features",
            "binary": True
        }

        StageFactory.set_params(stage, params)

        self.assertTrue(stage.isSet("inputCol"), msg="Param inputCols should be set by user")
        self.assertTrue(stage.isSet("outputCol"), msg="Param outputCol should be set by user")
        self.assertTrue(stage.isSet("binary"), msg="Param binary should be set by user")
        self.assertFalse(stage.isSet("numFeatures"), msg="Param numFeatures should NOT be set by user")

        self.assertEqual("words", stage.getInputCol(),
                         msg='Param inputCol should should have given value')
        self.assertEqual("features", stage.getOutputCol(),
                         msg='Param outputCol should should have given value')
        self.assertEqual(True, stage.getBinary(),
                         msg='Param binary should have given value')
        self.assertEqual(262144, stage.getNumFeatures(),
                         msg='Param numFeatures should have default value')

    def test_set_params_should_set_stage_default_params(self):
        stage = feature.HashingTF()
        params = {}

        StageFactory.set_params(stage, params)

        self.assertFalse(stage.isSet("inputCol"), msg="Param inputCols should NOT be set by user")
        self.assertFalse(stage.isSet("outputCol"), msg="Param outputCol should NOT be set by user")
        self.assertFalse(stage.isSet("binary"), msg="Param binary should NOT be set by user")
        self.assertFalse(stage.isSet("numFeatures"), msg="Param numFeatures should NOT be set by user")

    def test_set_params_should_throw_exception_if_wrong_param(self):
        stage = feature.HashingTF()
        params = {"wrongParam": 123}

        with self.assertRaises(TypeError):
            StageFactory.set_params(stage, params)

    def test_set_params_should_set_pipeline_params(self):
        stage = Pipeline()
        params = {"stages": [
            {
                "name": "feature.Tokenizer",
                "params": {
                    "inputCol": "text",
                    "outputCol": "words"
                }
            }
        ]
        }

        StageFactory.set_params(stage, params)

        self.assertTrue(stage.isSet("stages"), msg="Param stages should be set by user")
        self.assertTrue(isinstance(stage.getStages()[0], feature.Tokenizer),
                        msg="Param stages should have a Tokenizer object")

    def test_create_stage_should_retrun_wanted_stage(self):
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
                        "name": "Pipeline",
                        "params": {
                            "stages": [
                                {
                                    "name": "classification.LogisticRegression",
                                    "params": {
                                        "maxIter": 10,
                                        "regParam": 0.001
                                    }
                                }
                            ],
                        }
                    },
                ]
            }
        }

        stage = StageFactory.create_stage(my_stage)

        self.assertTrue(isinstance(stage, Pipeline))
        self.assertTrue(isinstance(stage.getStages()[0], feature.Tokenizer))
        sub_pipeline = stage.getStages()[1]
        self.assertTrue(isinstance(sub_pipeline, Pipeline))
        self.assertTrue(isinstance(sub_pipeline.getStages()[0], classification.LogisticRegression))


if __name__ == '__main__':
    unittest.main()
