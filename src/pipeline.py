from pyspark.sql import SparkSession

from src.step_factory import StepFactory

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("MLPipeline") \
        .getOrCreate()

    # stage_conf = { "name": "StageName", "params": { "parameter": value } }
    my_stage = {
        "name": "Pipeline",
        "params": {
            "stages": [
                {
                    "name": "feature.Tokenizer",
                    "params": {
                        "inputCol": "text",
                        "outputCol": "words"
                    }
                },
                {
                    "name": "feature.HashingTF",
                    "params": {
                        "inputCol": "words",
                        "outputCol": "features"
                    }
                },
                {
                    "name": "classification.LogisticRegression",
                    "params": {
                        "maxIter": 10,
                        "regParam": 0.001
                    }
                }
            ]
        }
    }

    my_fit_step = {
        "name": "fit",
        "params": {
            "dataset": {
                "path": "../test/resources/datasets/training.parquet",
                "format": "parquet"
            }
        },
        "stage": my_stage
    }

    # prediction = model.transform(test)
    my_predict_step = {
        "name": "transform",
        "params":
            {
                "dataset": {
                    "path": "../test/resources/datasets/test.parquet",
                    "format": "parquet"
                }
            },
        "stage": my_fit_step
    }

    prediction = StepFactory.create_step(spark, my_predict_step).execute()
    selected = prediction.select("id", "text", "probability", "prediction")
    for row in selected.collect():
        rid, text, prob, prediction = row
        print("(%d, %s) --> prob=%s, prediction=%f" % (rid, text, str(prob), prediction))

    spark.stop()
