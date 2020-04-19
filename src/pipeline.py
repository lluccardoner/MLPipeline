from pyspark.sql import SparkSession

from src.step_factory import StepFactory

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PipelineExample") \
        .getOrCreate()

    # Prepare training documents, which are labeled (id, text, label) tuples.
    training = spark.createDataFrame([
        (0, "a b c d e spark", 1.0),
        (1, "b d", 0.0),
        (2, "spark f g h", 1.0),
        (3, "hadoop mapreduce", 0.0)
    ], ["id", "text", "label"])

    # Prepare test documents, which are unlabeled (id, text) tuples.
    test = spark.createDataFrame([
        (4, "spark i j k"),
        (5, "l m n"),
        (6, "spark hadoop spark"),
        (7, "apache hadoop")
    ], ["id", "text"])

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
            "dataset": training
        },
        "stage": my_stage
    }

    # prediction = model.transform(test)
    my_predict_step = {
        "name": "transform",
        "params":
            {
                "dataset": test
            },
        "stage": my_fit_step
    }

    prediction = StepFactory.create_step(my_predict_step).execute()
    selected = prediction.select("id", "text", "probability", "prediction")
    for row in selected.collect():
        rid, text, prob, prediction = row
        print("(%d, %s) --> prob=%s, prediction=%f" % (rid, text, str(prob), prediction))

    spark.stop()
