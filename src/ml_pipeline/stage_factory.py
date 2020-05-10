from pyspark.ml import *

from ml_pipeline.dataset_factory import DatasetFactory


class StageFactory:

    def __init__(self):
        pass

    @staticmethod
    def get_stage(name):
        expr = name + "()"
        return eval(expr)

    @staticmethod
    def set_params(spark, stage, params):
        if not params:
            # Set default params
            stage.setParams()
        else:
            if isinstance(stage, Pipeline):
                params["stages"] = [StageFactory.create_stage(spark, s_conf) for s_conf in params["stages"]]
            stage.setParams(**params)

    @staticmethod
    def create_stage(spark, stage_conf):
        if not isinstance(stage_conf, dict):
            # Stage conf is already a stage
            return stage_conf

        name = stage_conf["name"]
        params = stage_conf["params"]

        if "dataset" in params and isinstance(params["dataset"], dict):
            # Create dataset if needed in stage parameters
            dataset_conf = params["dataset"]
            params["dataset"] = DatasetFactory.create_dataset(spark, dataset_conf)

        stage = StageFactory.get_stage(name)
        StageFactory.set_params(spark, stage, params)

        return stage
