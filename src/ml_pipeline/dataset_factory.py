class DatasetFactory:

    def __init__(self):
        pass

    @staticmethod
    def create_dataset(spark, dataset_conf):
        dataset = spark.read.load(**dataset_conf)

        return dataset
