# Databricks notebook source
# DBTITLE 0,Introduction
# MAGIC %md
# MAGIC #Introduction
# MAGIC Databand provides a few ways to log scripts and datasets in Spark. These options vary in complexity and the amount of code refactoring involved, but each option has its advantages. The below examples will highlight some of the differences as they relate to Databricks notebooks specifically.

# COMMAND ----------

# MAGIC %md
# MAGIC # Cluster configuration
# MAGIC Your Databricks cluster will need a few things in order to enable tracking:  
# MAGIC
# MAGIC 1. Install the Databand SDK by adding it as a PyPI package on your cluster (e.g. `databand[spark]==1.0.11.1`).
# MAGIC
# MAGIC 2. Specify the following environment variables at a minimum:
# MAGIC     ```
# MAGIC     DBND__TRACKING=True
# MAGIC     DBND__CORE__DATABAND_URL=https://your_env.databand.ai
# MAGIC     DBND__CORE__DATABAND_ACCESS_TOKEN=your_databand_access_token
# MAGIC     DBND__ENABLE__SPARK_CONTEXT_ENV=True
# MAGIC     ```
# MAGIC
# MAGIC 3. If using Databand's Spark listener, add the JVM agent to your cluster. You can download the JAR and either add it as a library through your cluster's configuration or place it in the folder of your choice, as long as that folder is accessible by the cluster. 
# MAGIC
# MAGIC 4. If using Databand's Spark listener, specify the following in the Spark configuration of your cluster. Be sure to change the path to the listener agent according to where you uploaded it:
# MAGIC     ```
# MAGIC     spark.sql.queryExecutionListeners ai.databand.spark.DbndSparkQueryExecutionListener
# MAGIC     spark.driver.extraJavaOptions -javaagent:/dbfs/FileStore/jars/cf3a7a64_588d_494e_bfc7_13fbb0aa9bb5-dbnd_agent_1_0_11_0_all-1518c.jar
# MAGIC     ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Constants
# MAGIC The below output path and JSON object will be used for all of the following examples. The file output will be overwritten with each command you run. **Be sure to run the following cell before you continue with the examples below**. 

# COMMAND ----------

# DBTITLE 0,Constants for these examples
OUTPUT_PATH = "dbfs:/data/people_output/"

PEOPLE = [
    {
        "first_name": "John",
        "last_name": "Smith",
        "age": 26,
    },
    {
        "first_name": "Harry",
        "last_name": "Thompson",
        "age": 49,
    },
    {
        "first_name": "Sam",
        "last_name": "Smith",
        "age": 36,
    },
]

# COMMAND ----------

# DBTITLE 0,Spark Listener
# MAGIC %md
# MAGIC #Spark listener
# MAGIC Databand's Spark listener is integrated at the cluster level. It will log all input/output operations executed on the cluster and send them to Databand as part of the run in which they were executed. When dealing with Databricks notebooks, it is important to note that Databricks does not treat notebook execution the same way that it would the execution of a script submitted to the cluster. For this reason, you will notice that a job tracked through an interactive notebook remains in a running state, even once execution has technically completed. Databand provides a single line of code that can be used as a workaround for this issue.
# MAGIC
# MAGIC Also, it is important to note that Spark does not consider every single dataframe command to be an input or output operation. In the examples below, a manually created JSON object will be used as the basis for a dataframe which will then be filtered and written to storage. In this case, Spark considers the write operation to be an output operation, but it does not consider the manual creation of the dataframe from the JSON object as an input operation. In a production scenario, your inputs will mainly be coming from file or table reads, so this is not a situation you're likely to encounter in the real world. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Listener only
# MAGIC The below will result in a job in your Databand environment called **Databricks Shell** with a randomly generated run name. This run will remain in a running state since Databricks notebooks do not inherrently close the Spark session once notebook execution has completed. Furthermore, since Databand interprets the run as still being in progress, no datasets that result from I/O operations are transmitted to Databand. 
# MAGIC
# MAGIC Please note that this behavior only applies when using Databricks notebooks. This is not an issue when submitting standalone scripts to a Databricks cluster. 

# COMMAND ----------

# DBTITLE 0,PySpark - Listener only
def get_people(json):
    people_df = spark.read.json(sc.parallelize(json))

    return people_df


def find_all_smiths(df):
    all_smiths = df[df["last_name"] == "Smith"]

    return all_smiths


def save_smiths(df):
    df.write.mode("overwrite").parquet(OUTPUT_PATH + "all_smiths.parquet")


people_df = get_people(PEOPLE)
all_smiths = find_all_smiths(people_df)
save_smiths(all_smiths)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flag the run as completed
# MAGIC Run the below command once you've validated the tracking behavior for the previous cell. This will result in the open run being marked as complete in Databand, as well as the transmission of any I/O operations picked up by the listener. 
# MAGIC
# MAGIC For this run, the **all_smiths** dataset should show up as a write operation within the run. Databand will capture the path of the write, the operation type (read vs. write), the record count, and the schema. 

# COMMAND ----------

spark._jvm.ai.databand.DbndWrapper.instance().afterPipeline()

# COMMAND ----------

import os
os.environ["DBND__VERBOSE"] = "False" 


# this will enable tracking for Python SDK, otherwise we are showing only "WARNINGS"
os.environ["DBND__TRACKING__LOGGER_DBND_LEVEL"]="WARNINGS"
os.environ["DBND__LOG__LEVEL"]="WARNINGA" #version <1.0.15

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding forced run completion
# MAGIC The results of executing the following cell should be the same as when you previously ran cells 7 and 9 in sequence, but the tracking context will close immediately without needing to run an extra cell. 

# COMMAND ----------

# DBTITLE 0,PySpark - Listener with forced run completion
from dbnd import task, log_metric, dbnd_tracking_start, dbnd_tracking_stop


@task
def get_people(json):
    people_df = spark.read.json(sc.parallelize(json))

    return people_df


@task
def find_all_smiths(df):
    all_smiths = df[df["last_name"] == "Smith"]

    return all_smiths


@task
def save_smiths(df):
    df.write.mode("overwrite").parquet(OUTPUT_PATH + "all_smiths.parquet")
    
    log_metric("test metric", 12345)


dbnd_tracking_start()

people_df = get_people(PEOPLE)
all_smiths = find_all_smiths(people_df)
save_smiths(all_smiths)

spark._jvm.ai.databand.DbndWrapper.instance().afterPipeline()  # Include this to signal to Databand that the run has completed. This is only required for notebooks.

dbnd_tracking_stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding custom job and run names
# MAGIC By default, the Spark listener will assign any I/O operations resulting from a manually-executed Databricks notebook to a generic pipeline called **Databricks Shell** with a randomly generated run name. If you would like to specify the job and/or run name within Databand, you can do so using environment variables:  
# MAGIC &nbsp;
# MAGIC - `DBND__RUN__NAME`: Used to set the pipeline name that will be displayed in Databand
# MAGIC - `DBND__RUN__JOB_NAME`: Used to set the run name displayed in Databand for an individual execution of your job
# MAGIC
# MAGIC As a best practice, we recommend using a dynamic run name to maintain uniqueness across runs. The simplest way to do this is by appending a timestamp to your run name through your orchestration tool. If this is not an option, you can choose not to set this variable which will continue generating random run names for you in the Databand UI. 
# MAGIC
# MAGIC There will not be an example of this included since it would require altering the cluster configuration, but feel free to validate this using your own cluster. 

# COMMAND ----------

# DBTITLE 0,Manual SDK Integration
# MAGIC %md
# MAGIC # Manual SDK integration
# MAGIC You may find more value by integrating Databand's SDK functions directly into your code. This will give you much greater control over establishing the tracking context, naming your jobs/runs/projects without changing the cluster configuration, tagging your functions as steps within your job, and logging column-level statistics. The tradeoff is that you will need to refactor your existing code, but you may find this worthwhile given the capabilities this provides. The following examples will showcase some of the features offered by manual SDK integration using the same code as the previous examples for the Spark listener.  
# MAGIC
# MAGIC *NOTE*: When not using the listener, you can remove the following parameters from the cluster configuration:  
# MAGIC &nbsp;  
# MAGIC - `spark.sql.queryExecutionListeners ai.databand.spark.DbndSparkQueryExecutionListener`
# MAGIC - `spark.driver.extraJavaOptions -javaagent:/dbfs/path/to/dbnd_agent_X_X_X_X_all.jar`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a tracking context
# MAGIC The most vital part of manual SDK integration is establishing a tracking context for your code. When working with notebooks, especially on an ad-hoc basis, you may have certain cells that are used for testing purposes, and you probably do not want those to be tracked within Databand. To control exactly which code gets tracked within your notebook, you can use the `dbnd_tracking` context manager from the Databand SDK. With this function, only the code contained within the tracking context gets sent to Databand. Furthermore, this function allows you to assign job, run, and project names to your code within the Databand UI without needing to change the cluster configuration at all.
# MAGIC
# MAGIC Notice in the following examples that you do not need to manually invoke `spark._jvm.ai.databand.DbndWrapper.instance().afterPipeline()` to indicate the completion of your job execution. Since we are specifically tracking the code within our tracking context, Databand knows that tracking has completed once the context is exited. One major advantage of this is that in the event of an error in your code execution, we can immediately report that your job failed in the Databand UI. When using the listener, if your code encounters an error prior to the stop signal being sent, you may end up with a job in your Databand UI that shows that it is running indefinitely.
# MAGIC
# MAGIC Lastly, it is important to note that any errors tied directly to the Databand SDK will not inhibit the execution of your code. For example, if your Databand environment is down for some reason, and Spark is unable to authenticate with it, your code contained within the tracking context will still execute -- you just won't see that run in the Databand UI. 

# COMMAND ----------

from dbnd import (
    dbnd_tracking,
)  # Import the context manager. This should already be available from when you installed the databand[spark] package on your cluster.

"""
Recall from cell 12 that it is recommended to always give unique names to your runs. An easy way to do this is by appending a timestamp to your run ID. 

Another option is to simply not specify a run name which will result in Databand assigning a random name to your run automatically. 
"""
from datetime import (
    datetime,
)  # To assist with assigning the current timestamp to your run for uniqueness


now = datetime.now()
run_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")


def get_people(json):
    people_df = spark.read.json(sc.parallelize(json))

    return people_df


def find_all_smiths(df):
    all_smiths = df[df["last_name"] == "Smith"]

    return all_smiths


def save_smiths(df):
    df.write.mode("overwrite").parquet(OUTPUT_PATH + "all_smiths.parquet")


with dbnd_tracking(
    job_name="my_databricks_job",  # The name that will appear on the Pipelines page in your Databand UI
    run_name="my_databricks_job: "
    + run_timestamp,  # The name that will appear on the Runs page in your Databand UI
    project_name="my_example_project",  # The project name assigned to your job. Projects can be used to group related jobs and allow for better filtering in the Databand UI.
):
    people_df = get_people(PEOPLE)
    all_smiths = find_all_smiths(people_df)
    save_smiths(all_smiths)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assigning task names to functions
# MAGIC You will have noticed in the previous examples that the pipeline graph shown on the run page in your Databand UI consists of a single task with the same name as your job. To give your job the look and feel of a typical pipeline, you can use Databand's `@task` decorator to flag the functions in your script as steps in your job.
# MAGIC
# MAGIC Each decorated function in your script will be represented by a step in your pipeline graph in the Databand UI. If dependencies are detected between functions (i.e. function_B uses the output of function_A as an input), Databand can draw the appropriate relationship lines between those functions in the graph view.
# MAGIC
# MAGIC In our example, using task decorators should result in a graph that looks similar to the below sequence:  
# MAGIC &nbsp;  
# MAGIC [get_people] --> [find_all_smiths] --> [save_smiths]

# COMMAND ----------

from dbnd import dbnd_tracking, task  # Import the task decorator

from datetime import datetime


now = datetime.now()
run_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")


@task  # Tag each of your functions with the @task decorator so they show up as pipeline steps in the Databand UI
def get_people(json):
    people_df = spark.read.json(sc.parallelize(json))

    return people_df


@task
def find_all_smiths(df):
    all_smiths = df[df["last_name"] == "Smith"]

    return all_smiths


@task
def save_smiths(df):
    df.write.mode("overwrite").parquet(OUTPUT_PATH + "all_smiths.parquet")


with dbnd_tracking(
    job_name="my_databricks_job",
    run_name="my_databricks_job: " + run_timestamp,
    project_name="my_example_project",
):
    people_df = get_people(PEOPLE)
    all_smiths = find_all_smiths(people_df)
    save_smiths(all_smiths)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logging datasets
# MAGIC When we used Databand's Spark listener in the first set of examples, we were able to capture some high level metadata about the dataset operations happening in our code. The listener is able to automatically log the dataset path, operation type, record count, and schema of any I/O operations that happen on a cluster. In cases where you need more control over *which* datasets get logged, or you want richer metadata such as column-level statistics, you are able to manually track your datasets using Databand's `dataset_op_logger`. 
# MAGIC
# MAGIC Similar to `dbnd_tracking`, the `dataset_op_logger` function works best as a context manager for your dataset operations. Below is an example of the usage of `dataset_op_logger` and its parameters:  
# MAGIC    
# MAGIC
# MAGIC ```python
# MAGIC with dataset_op_logger(
# MAGIC     "dbfs://path/to/some/file.csv", # The full path of your dataset which will become its unique identifier within the Databand UI. This can be a file, table, or URL. 
# MAGIC     "read", # Whether the operation is a "read" or a "write"
# MAGIC     with_schema=True, # Log the schema of the dataset, default: True
# MAGIC     with_stats=True, # Log the column-level statistics of the dataset, default: True
# MAGIC     with_preview=True, # Transmit a preview of the dataset to Databand to view in the UI, default: False
# MAGIC     with_partition=True, # If the dataset path is partitioned, this will ignore partitioned subfolders (e.g. /column=value/), default: False
# MAGIC ) as logger:
# MAGIC     people_df = spark.read.json(sc.parallelize(json))
# MAGIC     logger.set(data=people_df) # Specify the dataset object that should be logged
# MAGIC ```
# MAGIC
# MAGIC When using `dataset_op_logger`, you should be sure to only include the steps relevant to the dataset operation within the logging context. If an error happens in your code within the logging context, Databand will flag that as a failed dataset operation in the Databand UI. This means that including things unrelated to your dataset operation could potentially create false positives in terms of failed operations. 
# MAGIC
# MAGIC *NOTE*: When logging column-level statistics, the calculation of statistics is done at runtime, and all column profiling must be completed before your script continues to any subsequent steps. It is very important to consider the tradeoffs between the Spark listener and manual SDK logging when dealing with large (i.e. hundreds of columns, millions of records) datasets:  
# MAGIC &nbsp;  
# MAGIC - The Spark listener will add no processing overhead to your Spark script, but you will not have access to column-level statistics.
# MAGIC - Manual SDK integration gives access to column-level statistics, but calculating those statistics can add varying amounts of overhead to your Spark script.
# MAGIC
# MAGIC In general, we find that most of our customers tend to use the Spark listener due to the fact it that has no performance impact while still providing the ability to generate valuable alerts on your datasets such as schema changes and record count anomalies. 

# COMMAND ----------

from dbnd import dbnd_tracking, task, dataset_op_logger # Import the dataset logger

from datetime import datetime


now = datetime.now()
run_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")


@task
def get_people(json):
    """
    In the example below, we provide a fake path to our JSON object since we are manually creating it as part of the notebook. In a real example, this would be the file         path, table path, or URL from which your data is being read.

    The only mandatory parameters required for dataset_op_logger are the path and the operation type. We are fine with the defaults for the other parameters in this case.
    """
    with dataset_op_logger(
        "dbfs:/path/to/people.json",
        "read",
    ) as logger:
        people_df = spark.read.json(sc.parallelize(json))
        logger.set(data=people_df)

    return people_df


@task
def find_all_smiths(df):
    all_smiths = df[df["last_name"] == "Smith"]

    return all_smiths


@task
def save_smiths(df):
    # The below dataset will retain all of the metadata captured in the listener examples, but it will now display column-level statistics as well.
    with dataset_op_logger(
        OUTPUT_PATH + "all_smiths.parquet",
        "write",
    ) as logger:
        df.write.mode("overwrite").parquet(OUTPUT_PATH + "all_smiths.parquet")
        logger.set(data=df)


with dbnd_tracking(
    job_name="my_databricks_job",
    run_name="my_databricks_job: " + run_timestamp,
    project_name="my_example_project",
):
    people_df = get_people(PEOPLE)
    all_smiths = find_all_smiths(people_df)
    save_smiths(all_smiths)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logging custom metrics
# MAGIC Databand logs several metrics when using `dbnd_tracking` and `dataset_op_logger`. If you would like to log custom metrics, you can do so using the `log_metric` function. Custom metrics will show up in the Databand UI as part of the tracked run, and you will be able to create alerts on them the same way that you can with Databand's native metrics. 

# COMMAND ----------

from dbnd import dbnd_tracking, task, dataset_op_logger, log_metric # Import the metric logger

from datetime import datetime


now = datetime.now()
run_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")


@task
def get_people(json):
    with dataset_op_logger(
        "dbfs:/path/to/people.json",
        "read",
    ) as logger:
        people_df = spark.read.json(sc.parallelize(json))
        logger.set(data=people_df)

    return people_df


@task
def find_all_smiths(df):
    all_smiths = df[df["last_name"] == "Smith"]
    
    total_smiths = all_smiths.count()
    log_metric("Total Smiths", total_smiths) # The first parameter will be the name of the metric in the Databand UI, the second is the calculation to log

    return all_smiths


@task
def save_smiths(df):
    with dataset_op_logger(
        OUTPUT_PATH + "all_smiths.parquet",
        "write",
    ) as logger:
        df.write.mode("overwrite").parquet(OUTPUT_PATH + "all_smiths.parquet")
        logger.set(data=df)


with dbnd_tracking(
    job_name="my_databricks_job",
    run_name="my_databricks_job: " + run_timestamp,
    project_name="my_example_project",
):
    people_df = get_people(PEOPLE)
    all_smiths = find_all_smiths(people_df)
    save_smiths(all_smiths)

# COMMAND ----------

# MAGIC %md
# MAGIC # Combining the listener with manual SDK tracking
# MAGIC Databand's Spark listener and SDK functions are not mutually exclusive. You can combine them if desired to get the best of both worlds. 
# MAGIC
# MAGIC For the example below, re-enable the Spark listener if you disabled it for the preceding examples. In this example, we will combine the ease of dataset logging from the Spark listener with the flexibility provided by SDK functions to enable our tracking context and decorate our tasks. 

# COMMAND ----------

from dbnd import dbnd_tracking, task

from datetime import datetime


now = datetime.now()
run_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")


@task
def get_people(json):
    # Remember that the people_df dataset does not get picked up by the listener; however, we have the option to manually log it with the SDK if we'd like.
    people_df = spark.read.json(sc.parallelize(json))

    return people_df


@task
def find_all_smiths(df):
    all_smiths = df[df["last_name"] == "Smith"]

    return all_smiths


@task
def save_smiths(df):
    # Even though we are no longer using dataset_op_logger, this dataset operation will still be logged since the listener is enabled, and it will display it in the Databand UI under the job and run specified below.
    df.write.mode("overwrite").parquet(OUTPUT_PATH + "all_smiths.parquet")


with dbnd_tracking(
    job_name="my_databricks_job",
    run_name="my_databricks_job: " + run_timestamp,
    project_name="my_example_project",
):
    people_df = get_people(PEOPLE)
    all_smiths = find_all_smiths(people_df)
    save_smiths(all_smiths)