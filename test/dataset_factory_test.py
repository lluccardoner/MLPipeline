import unittest

from pyspark.sql import SparkSession

from dataset_factory import DatasetFactory


class DatasetFactoryTest(unittest.TestCase):
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

    def test_create_dataset_should_load_parquet(self):
        dataset_conf = {
            "path": "resources/datasets/test.parquet",
            "format": "parquet"
        }

        dataset = DatasetFactory.create_dataset(self.spark, dataset_conf)

        self.assertEqual(dataset.columns, ["id", "text"])
        self.assertEqual(dataset.count(), 4)

    def test_create_dataset_should_load_csv(self):
        dataset_conf = {
            "path": "resources/datasets/test.csv",
            "format": "csv",
            "sep": ",",
            "header": True
        }

        dataset = DatasetFactory.create_dataset(self.spark, dataset_conf)

        self.assertEqual(dataset.columns, ["id", "text"])
        self.assertEqual(dataset.count(), 4)


if __name__ == '__main__':
    unittest.main()
