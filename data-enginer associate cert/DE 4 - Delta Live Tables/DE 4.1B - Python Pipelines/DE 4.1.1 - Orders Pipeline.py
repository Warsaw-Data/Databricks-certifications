# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Fundamentals of DLT Python Syntax
# MAGIC
# MAGIC This notebook demonstrates using Delta Live Tables (DLT) to process raw data from JSON files landing in cloud object storage through a series of tables to drive analytic workloads in the lakehouse. Here we demonstrate a medallion architecture, where data is incrementally transformed and enriched as it flows through a pipeline. This notebook focuses on the Python syntax of DLT rather than this architecture, but a brief overview of the design:
# MAGIC
# MAGIC * The bronze table contains raw records loaded from JSON enriched with data describing how records were ingested
# MAGIC * The silver table validates and enriches the fields of interest
# MAGIC * The gold table contains aggregate data to drive business insights and dashboarding
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, students should feel comfortable:
# MAGIC * Declaring Delta Live Tables
# MAGIC * Ingesting data with Auto Loader
# MAGIC * Using parameters in DLT Pipelines
# MAGIC * Enforcing data quality with constraints
# MAGIC * Adding comments to tables
# MAGIC * Describing differences in syntax and execution of live tables and streaming live tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## About DLT Library Notebooks
# MAGIC
# MAGIC DLT syntax is not intended for interactive execution in a notebook. This notebook will need to be scheduled as part of a DLT pipeline for proper execution. 
# MAGIC
# MAGIC At the time this notebook was written, the current Databricks runtime does not include the **`dlt`** module, so trying to execute any DLT commands in a notebook will fail. 
# MAGIC
# MAGIC We'll discuss developing and troubleshooting DLT code later in the course.
# MAGIC
# MAGIC ## Parameterization
# MAGIC
# MAGIC During the configuration of the DLT pipeline, a number of options were specified. One of these was a key-value pair added to the **Configurations** field.
# MAGIC
# MAGIC Configurations in DLT pipelines are similar to parameters in Databricks Jobs, but are actually set as Spark configurations.
# MAGIC
# MAGIC In Python, we can access these values using **`spark.conf.get()`**.
# MAGIC
# MAGIC Throughout these lessons, we'll set the Python variable **`source`** early in the notebook and then use this variable as necessary in the code.
# MAGIC
# MAGIC ## A Note on Imports
# MAGIC
# MAGIC The **`dlt`** module should be explicitly imported into your Python notebook libraries.
# MAGIC
# MAGIC Here, we should importing **`pyspark.sql.functions`** as **`F`**.
# MAGIC
# MAGIC Some developers import **`*`**, while others will only import the functions they need in the present notebook.
# MAGIC
# MAGIC These lessons will use **`F`** throughout so that students clearly know which methods are imported from this library.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables as DataFrames
# MAGIC
# MAGIC There are two distinct types of persistent tables that can be created with DLT:
# MAGIC * **Live tables** are materialized views for the lakehouse; they will return the current results of any query with each refresh
# MAGIC * **Streaming live tables** are designed for incremental, near-real time data processing
# MAGIC
# MAGIC Note that both of these objects are persisted as tables stored with the Delta Lake protocol (providing ACID transactions, versioning, and many other benefits). We'll talk more about the differences between live tables and streaming live tables later in the notebook.
# MAGIC
# MAGIC Delta Live Tables introduces a number of new Python functions that extend familiar PySpark APIs.
# MAGIC
# MAGIC At the heart of this design, the decorator **`@dlt.table`** is added to any Python function that returns a Spark DataFrame. (**NOTE**: This includes Koalas DataFrames, but these won't be covered in this course.)
# MAGIC
# MAGIC If you're used to working with Spark and/or Structured Streaming, you'll recognize the majority of the syntax used in DLT. The big difference is that you'll never see any methods or options for DataFrame writers, as this logic is handled by DLT.
# MAGIC
# MAGIC As such, the basic form of a DLT table definition will look like:
# MAGIC
# MAGIC **`@dlt.table`**<br/>
# MAGIC **`def <function-name>():`**<br/>
# MAGIC **`    return (<query>)`**</br>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Streaming Ingestion with Auto Loader
# MAGIC
# MAGIC Databricks has developed the [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html) functionality to provide optimized execution for incrementally loading data from cloud object storage into Delta Lake. Using Auto Loader with DLT is simple: just configure a source data directory, provide a few configuration settings, and write a query against your source data. Auto Loader will automatically detect new data files as they land in the source cloud object storage location, incrementally processing new records without the need to perform expensive scans and recomputing results for infinitely growing datasets.
# MAGIC
# MAGIC Auto Loader can be combined with Structured Streaming APIs to perform incremental data ingestion throughout Databricks by configuring the **`format("cloudFiles")`** setting. In DLT, you'll only configure settings associated with reading data, noting that the locations for schema inference and evolution will also be configured automatically if those settings are enabled.
# MAGIC
# MAGIC The query below returns a streaming DataFrame from a source configured with Auto Loader.
# MAGIC
# MAGIC In addition to passing **`cloudFiles`** as the format, here we specify:
# MAGIC * The option **`cloudFiles.format`** as **`json`** (this indicates the format of the files in the cloud object storage location)
# MAGIC * The option **`cloudFiles.inferColumnTypes`** as **`True`** (to auto-detect the types of each column)
# MAGIC * The path of the cloud object storage to the **`load`** method
# MAGIC * A select statement that includes a couple of **`pyspark.sql.functions`** to enrich the data alongside all the source fields
# MAGIC
# MAGIC By default, **`@dlt.table`** will use the name of the function as the name for the target table.

# COMMAND ----------

@dlt.table
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/orders")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validating, Enriching, and Transforming Data
# MAGIC
# MAGIC DLT allows users to easily declare tables from results of any standard Spark transformations. DLT adds new functionality for data quality checks and provides a number of options to allow users to enrich the metadata for created tables.
# MAGIC
# MAGIC Let's break down the syntax of the query below.
# MAGIC
# MAGIC ### Options for **`@dlt.table()`**
# MAGIC
# MAGIC There are <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-python-ref.html#create-table" target="_blank">a number of options</a> that can be specified during table creation. Here, we use two of these to annotate our dataset.
# MAGIC
# MAGIC ##### **`comment`**
# MAGIC
# MAGIC Table comments are a standard for relational databases. They can be used to provide useful information to users throughout your organization. In this example, we write a short human-readable description of the table that describes how data is being ingested and enforced (which could also be gleaned from reviewing other table metadata).
# MAGIC
# MAGIC ##### **`table_properties`**
# MAGIC
# MAGIC This field can be used to pass any number of key/value pairs for custom tagging of data. Here, we set the value **`silver`** for the key **`quality`**.
# MAGIC
# MAGIC Note that while this field allows for custom tags to be arbitrarily set, it is also used for configuring number of settings that control how a table will perform. While reviewing table details, you may also encounter a number of settings that are turned on by default any time a table is created.
# MAGIC
# MAGIC ### Data Quality Constraints
# MAGIC
# MAGIC The Python version of DLT uses decorator functions to set <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html#delta-live-tables-data-quality-constraints" target="_blank">data quality constraints</a>. We'll see a number of these throughout the course.
# MAGIC
# MAGIC DLT uses simple boolean statements to allow quality enforcement checks on data. In the statement below, we:
# MAGIC * Declare a constraint named **`valid_date`**
# MAGIC * Define the conditional check that the field **`order_timestamp`** must contain a value greater than January 1, 2021
# MAGIC * Instruct DLT to fail the current transaction if any records violate the constraint by using the decorator **`@dlt.expect_or_fail()`**
# MAGIC
# MAGIC Each constraint can have multiple conditions, and multiple constraints can be set for a single table. In addition to failing the update, constraint violation can also automatically drop records or just record the number of violations while still processing these invalid records.
# MAGIC
# MAGIC ### DLT Read Methods
# MAGIC
# MAGIC The Python **`dlt`** module provides the **`read()`** and **`read_stream()`** methods to easily configure references to other tables and views in your DLT Pipeline. This syntax allows you to reference these datasets by name without any database reference. You can also use **`spark.table("LIVE.<table_name.")`**, where **`LIVE`** is a keyword substituted for the database being referenced in the DLT Pipeline.

# COMMAND ----------

@dlt.table(
    comment = "Append only orders with valid timestamps",
    table_properties = {"quality": "silver"})
@dlt.expect_or_fail("valid_date", F.col("order_timestamp") > "2021-01-01")
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
            .select(
                "processing_time",
                "customer_id",
                "notifications",
                "order_id",
                F.col("order_timestamp").cast("timestamp").alias("order_timestamp")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live Tables vs. Streaming Live Tables
# MAGIC
# MAGIC The two functions we've reviewed so far have both created streaming live tables. Below, we see a simple function that returns a live table (or materialized view) of some aggregated data.
# MAGIC
# MAGIC Spark has historically differentiated between batch queries and streaming queries. Live tables and streaming live tables have similar differences.
# MAGIC
# MAGIC Note that these table types inherit the syntax (as well as some of the limitations) of the PySpark and Structured Streaming APIs.
# MAGIC
# MAGIC Below are some of the differences between these types of tables.
# MAGIC
# MAGIC ### Live Tables
# MAGIC * Always "correct", meaning their contents will match their definition after any update.
# MAGIC * Return same results as if table had just been defined for first time on all data.
# MAGIC * Should not be modified by operations external to the DLT Pipeline (you'll either get undefined answers or your change will just be undone).
# MAGIC
# MAGIC ### Streaming Live Tables
# MAGIC * Only supports reading from "append-only" streaming sources.
# MAGIC * Only reads each input batch once, no matter what (even if joined dimensions change, or if the query definition changes, etc).
# MAGIC * Can perform operations on the table outside the managed DLT Pipeline (append data, perform GDPR, etc).

# COMMAND ----------

@dlt.table
def orders_by_date():
    return (
        dlt.read("orders_silver")
            .groupBy(F.col("order_timestamp").cast("date").alias("order_date"))
            .agg(F.count("*").alias("total_daily_orders"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC By reviewing this notebook, you should now feel comfortable:
# MAGIC * Declaring Delta Live Tables
# MAGIC * Ingesting data with Auto Loader
# MAGIC * Using parameters in DLT Pipelines
# MAGIC * Enforcing data quality with constraints
# MAGIC * Adding comments to tables
# MAGIC * Describing differences in syntax and execution of live tables streaming live tables
# MAGIC
# MAGIC In the next notebook, we'll continue learning about these syntactic constructs while adding a few new concepts.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>