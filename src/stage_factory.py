from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer


class StageFactory:

    def __init__(self):
        self.classes = self._register_classes()

    def _register_classes(self):
        return {
            'Pipeline': Pipeline,
            'Tokenizer': Tokenizer,
            'HashingTF': HashingTF,
            'LogisticRegression': LogisticRegression,
        }

    def _get_stage(self, name):
        return self.classes[name]()

    def _set_params(self, stage, params):
        if isinstance(stage, Pipeline):
            params["stages"] = [self.create_stage(s_conf) for s_conf in params["stages"]]
        stage.setParams(**params)

    def create_stage(self, stage_conf):
        name = stage_conf["name"]
        params = stage_conf["params"]

        stage = self._get_stage(name)
        self._set_params(stage, params)

        return stage
