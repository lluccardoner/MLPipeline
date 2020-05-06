from dataset_factory import DatasetFactory
from src.stage_factory import StageFactory
from step import Step


class StepFactory:

    def __init__(self):
        pass

    @staticmethod
    def create_step(spark, step_conf):
        name = step_conf["name"]
        params = step_conf["params"]
        stage = step_conf["stage"]

        if isinstance(stage, dict) and "stage" in stage:
            # Stage comes from an executed step
            stage = StepFactory.create_step(spark, stage).execute()
        else:
            # Create a stage
            stage = StageFactory.create_stage(spark, stage)

        if "dataset" in params and isinstance(params["dataset"], dict):
            # Create dataset if needed in step parameters
            dataset_conf = params["dataset"]
            params["dataset"] = DatasetFactory.create_dataset(spark, dataset_conf)

        return Step(name, params, stage)
