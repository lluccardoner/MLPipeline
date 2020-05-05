from pyspark.sql import SparkSession

from step_factory import StepFactory

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("MLPipeline") \
        .getOrCreate()

    # Prepare training data from a list of (label, features) tuples.
    # Create a LogisticRegression instance. This instance is an Estimator.
    # Learn a LogisticRegression model. This uses the parameters stored in lr.
    # model1 = lr.fit(training)
    model1_step = {
        "name": "fit",
        "params": {
            "dataset": {
                "path": "dataset/estimator_transform_param/training.parquet",
                "format": "parquet"
            }
        },
        "stage": {
            "name": "classification.LogisticRegression",
            "params": {
                "maxIter": 10,
                "regParam": 0.01
            }
        }
    }

    model1 = StepFactory.create_step(spark, model1_step).execute()

    # Since model1 is a Model (i.e., a transformer produced by an Estimator),
    # we can view the parameters it used during fit().
    # This prints the parameter (name: value) pairs, where names are unique IDs for this
    # LogisticRegression instance.
    print("Model 1 was fit using parameters: ")
    print(model1.extractParamMap())

    # Now learn a new model using the paramMapCombined parameters.
    # paramMapCombined overrides all parameters set earlier via lr.set* methods.
    # model2 = lr.fit(training, paramMapCombined)
    model2_step = {
        "name": "fit",
        "params": {
            "dataset": {
                "path": "dataset/estimator_transform_param/training.parquet",
                "format": "parquet"
            }
        },
        "stage": {
            "name": "classification.LogisticRegression",
            "params": {
                "maxIter": 30,
                "regParam": 0.1,
                "threshold": 0.55,
                "probabilityCol": "myProbability"
            }
        }
    }

    model2 = StepFactory.create_step(spark, model2_step).execute()
    print("Model 2 was fit using parameters: ")
    print(model2.extractParamMap())

    # Make predictions on test data using the Transformer.transform() method.
    # LogisticRegression.transform will only use the 'features' column.
    # Note that model2.transform() outputs a "myProbability" column instead of the usual
    # 'probability' column since we renamed the lr.probabilityCol parameter previously.
    # prediction = model2.transform(test)
    prediction_step = {
        "name": "transform",
        "params": {
            "dataset": {
                "path": "dataset/estimator_transform_param/test.parquet",
                "format": "parquet"
            }
        },
        "stage": model2
    }

    prediction = StepFactory.create_step(spark, prediction_step).execute()
    result = prediction.select("features", "label", "myProbability", "prediction") \
        .collect()

    for row in result:
        print("features=%s, label=%s -> prob=%s, prediction=%s"
              % (row.features, row.label, row.myProbability, row.prediction))
