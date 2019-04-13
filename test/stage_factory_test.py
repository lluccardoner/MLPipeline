import unittest

from pyspark.ml.feature import Tokenizer
from pyspark.sql import SparkSession

from stage_factory import StageFactory


class StageFactoryTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession \
            .builder \
            .appName("StageFactoryTest") \
            .getOrCreate()

    def test_get_stage_should_return_stage_from_registered_classes(self):
        factory = StageFactory()
        factory.classes = {'Tokenizer': Tokenizer}

        stage = factory._get_stage('Tokenizer')

        self.assertIsInstance(stage, Tokenizer,
                              msg=f"Stage {stage} is not instance of {Tokenizer.__class__}")


if __name__ == '__main__':
    unittest.main()
