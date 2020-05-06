# MLPipeline
This project aims to build a library that gives the framework to create [Spark](https://spark.apache.org) pipelines without coding, just by using a configuration file.

Spark [pipelines](https://spark.apache.org/docs/latest/ml-pipeline.html) are used to combine multiple algorithms into a single pipeline, or workflow.
 
 **Motivation**: The motivation of this project is to be able to try different ML pipelines fast without the need to change the code.
 
 ## Usage
 Create a config file with the ML pipeline that you want to run and execute the following command:
 
 ```
 python app.py path/to/config.json
 ```
 
 ## ML Pipeline configuration
 
### Step configuration
 
```
step_config = {
    "name": step_name,
    "params": step_params,
    "stage": step_stage
}
```

* `step_name` is the name of the method that will be executed on the `ste_stage`. Possible values are: `fit`, `transform`, `predict`, `save`, `load`.
* `step_params` are the params of the method that will be executed in `step_name` (i.e dataset).
* `step_stage` is the stage to execute the step on (see Stage Configuration). The stage could be the output of another step.

### Stage configuration

```
stage_config = {
        "name": stage_name,
        "params": stage_params 
}
```

* `stage_name` is the name of the stage that will be created. It must have the package paths relative to ``pyspark.ml`` (i.e ``feature.Tokenizer``, ``classification.LogisticRegression``)
* `stage_params` are the params of the stage (i.e input and output columns)

### Dataset configuration
The datset config is given inside the `parmas` of a step or stage.

```
dataset_config = {
        "path": datset_path,
        "format": dataset_format 
}
```

* `datset_path` is the name of the stage that will be created. It must have the package paths relative to `pyspark.ml` (i.e `feature.Tokenizer`, `classification.LogisticRegression`).
* `dataset_format` is the format of the dataset. It could be: `parquet`, `csv`.

### Examples

Fit a LR model with a given dataset:

```
{
    "name": "fit",
    "params": {
        "dataset": {
            "path": "dataset/training",
            "format": "parquet"
        }
    },
    "stage": {
        "name": "classification.LogisticRegression",
        "params": {
            "maxIter": 10,
            "regParam": 0.001
            }
        }
}
```

Create a pipeline with different stages:

```
{
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
```

## Tasks 
 The goal is to be able to do the following things only with a single config file:
 * [DONE] Create pipelines with multiple stages
 * [DONE] Set stages' attributes
 * [DONE] Perform pipeline steps
    * [DONE] fit, transform (predict)
    * [DONE] save, load
 * [DONE] Dataset operations: 
    * [DONE] load, transform
    * [TODO] save 
 * [TODO] Perform cross validation on a pipeline 
 * [TODO] Perform hyper-parameter tuning on a pipeline 
 * [DONE] Load config from different sources
    * [DONE] JSON
    * [TODO] YAML and others
 * [TODO] Combine all these functionalities in one python library