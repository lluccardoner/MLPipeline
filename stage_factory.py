from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer


class StageFactory:

    def __init__(self):
        pass

    @staticmethod
    def create_stage(stage_conf):
        stage = None
        name = stage_conf["name"]
        params = stage_conf["params"]

        if name == "Pipeline":
            params["stages"] = [StageFactory.create_stage(s_conf) for s_conf in params["stages"]]
            stage = Pipeline().setParams(**params)
        elif name == "Tokenizer":
            stage = Tokenizer().setParams(**params)
        elif name == "HashingTF":
            stage = HashingTF().setParams(**params)
        elif name == "LogisticRegression":
            stage = LogisticRegression().setParams(**params)

        return stage
