from src.stage_factory import StageFactory
from step import Step


class StepFactory:

    def __init__(self):
        pass

    @staticmethod
    def create_step(step_conf):
        name = step_conf["name"]
        params = step_conf["params"]
        stage = step_conf["stage"]

        if "stage" in stage:
            # Stage comes from an executed step
            stage = StepFactory.create_step(stage).execute()
        else:
            # Create a stage
            stage = StageFactory.create_stage(stage)
        return Step(name, params, stage)
