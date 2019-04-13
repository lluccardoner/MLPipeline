from pyspark.ml import *


class StageFactory:

    def __init__(self):
        pass

    @staticmethod
    def get_stage(name):
        return eval(name + "()")

    @staticmethod
    def set_params(stage, params):
        if isinstance(stage, Pipeline):
            params["stages"] = [StageFactory.create_stage(s_conf) for s_conf in params["stages"]]
        stage.setParams(**params)

    @staticmethod
    def create_stage(stage_conf):
        name = stage_conf["name"]
        params = stage_conf["params"]

        stage = StageFactory.get_stage(name)
        StageFactory.set_params(stage, params)

        return stage
