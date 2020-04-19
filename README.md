# MLPipeline
This project aims to build a library that gives the framework to create [Spark](https://spark.apache.org) pipelines without coding, just by using a configuration file.

Spark [pipelines](https://spark.apache.org/docs/latest/ml-pipeline.html) are used to combine multiple algorithms into a single pipeline, or workflow.

 ## Goal
 The starting point is to take the [pipeline example](https://github.com/apache/spark/blob/master/examples/src/main/python/ml/pipeline_example.py) and change all the parts so they can be done by just reading the config file.
 
 The goal is to be able to do the following things only with a single config file:
 * [DONE] Create pipelines with multiple stages
 * [DONE] Set stages' attributes
 * [IN PROGRESS] Perform pipeline steps
    * [DONE] fit, transform (predict)
    * [TODO] save, load
 * [TODO] Dataset operations: load, transform and save 
 * [TODO] Perform cross validation on a pipeline 
 * [TODO] Perform hyper-parameter tuning on a pipeline 
 * [TODO] Load config from different sources: JSON, YAML.. 
 
 As stated before, the main goal is to couple all these functionalities in one library.
 
 ## Step configuration
 
```
step_config = {
    "name": step_name,
    "params": step_params,
    "stage": step_stage
}
```

* ``step_name`` is the name of the method that will be executed on the step stage (i.e ``fit``, ``transform``, ``save``, ``load``)
* ``step_params`` are the params of the method that will be executed (i.e dataset)
* ``step_stage`` is the stage to execute the step on. The stage could be the output of another step

## Stage configuration

```
stage_config = {
        "name": stage_name,
        "params": stage_params 
}
```

* ``stage_name`` is the name of the stage that will be created. It must have the package paths relative to ``pyspark.ml`` (i.e ``feature.Tokenizer``, ``classification.LogisticRegression``)
* ``stage_params`` are the params of the stage (i.e input and utput columns)