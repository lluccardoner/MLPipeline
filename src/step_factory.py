from step import Step


class StepFactory:

    def __init__(self):
        pass

    @staticmethod
    def create_step(step_conf):
        name = step_conf["name"]
        params = step_conf['params']

        return Step(name, params)
