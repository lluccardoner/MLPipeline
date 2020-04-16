from src.stage_factory import StageFactory
from step import Step
from pyspark.ml import Estimator, Transformer


class StepFactory:

    def __init__(self):
        pass

    @staticmethod
    def create_step(step_conf):
        name = step_conf["name"]
        params = step_conf["params"]
        stage = step_conf["stage"]

        if not issubclass(stage.__class__, (Estimator, Transformer)):
            stage = StageFactory.create_stage(stage)

        return Step(name, params, stage)
