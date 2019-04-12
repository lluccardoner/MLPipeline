#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Pipeline Example.
"""

# $example on$
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PipelineExample") \
        .getOrCreate()

    # $example on$
    # Prepare training documents from a list of (id, text, label) tuples.

    # Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
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

    def create_stage(stage_conf):
        stage = None
        name = stage_conf["name"]
        params = stage_conf["params"]

        if name == "Pipeline":
            params["stages"] = [create_stage(s_conf) for s_conf in params["stages"]]
            stage = Pipeline().setParams(**params)
        elif name == "Tokenizer":
            stage = Tokenizer().setParams(**params)
        elif name == "HashingTF":
            stage = HashingTF().setParams(**params)
        elif name == "LogisticRegression":
            stage = LogisticRegression().setParams(**params)

        return stage


    pipeline = create_stage(my_stage)
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
    # $example off$

    spark.stop()
