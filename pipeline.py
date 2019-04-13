from pyspark.sql import SparkSession

from stage_factory import StageFactory

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PipelineExample") \
        .getOrCreate()

    training = spark.createDataFrame([
        (0, "a b c d e spark", 1.0),
        (1, "b d", 0.0),
        (2, "spark f g h", 1.0),
        (3, "hadoop mapreduce", 0.0)
    ], ["id", "text", "label"])

    # stage_conf = { "name": "StageName", "params": { "parameter": value } }
    my_stage = {
        "name": "Pipeline",
        "params": {
            "stages": [
                {
                    "name": "Tokenizer",
                    "params": {
                        "inputCol": "text",
                        "outputCol": "words"
                    }
                },
                {
                    "name": "HashingTF",
                    "params": {
                        "inputCol": "words",
                        "outputCol": "features"
                    }
                },
                {
                    "name": "LogisticRegression",
                    "params": {
                        "maxIter": 10,
                        "regParam": 0.001
                    }
                }
            ]
        }
    }

    pipeline = StageFactory.create_stage(my_stage)
    print(pipeline.explainParams())

    # Fit the pipeline to training documents.
    model = pipeline.fit(training)

    # Prepare test documents, which are unlabeled (id, text) tuples.
    test = spark.createDataFrame([
        (4, "spark i j k"),
        (5, "l m n"),
        (6, "spark hadoop spark"),
        (7, "apache hadoop")
    ], ["id", "text"])

    # Make predictions on test documents and print columns of interest.
    prediction = model.transform(test)
    selected = prediction.select("id", "text", "probability", "prediction")
    for row in selected.collect():
        rid, text, prob, prediction = row
    print("(%d, %s) --> prob=%s, prediction=%f" % (rid, text, str(prob), prediction))

    spark.stop()
