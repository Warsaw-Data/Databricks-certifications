# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Reader & Writer
# MAGIC ##### Objectives
# MAGIC 1. Read from CSV files
# MAGIC 1. Read from JSON files
# MAGIC 1. Write DataFrame to files
# MAGIC 1. Write DataFrame to tables
# MAGIC 1. Write DataFrame to a Delta table
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">DataFrameReader</a>: **`csv`**, **`json`**, **`option`**, **`schema`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">DataFrameWriter</a>: **`mode`**, **`option`**, **`parquet`**, **`format`**, **`saveAsTable`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.StructType.html#pyspark.sql.types.StructType" target="_blank">StructType</a>: **`toDDL`**
# MAGIC
# MAGIC ##### Spark Types
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#data-types" target="_blank">Types</a>: **`ArrayType`**, **`DoubleType`**, **`IntegerType`**, **`LongType`**, **`StringType`**, **`StructType`**, **`StructField`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.07

# COMMAND ----------

# MAGIC %md ## DataFrameReader
# MAGIC Interface used to load a DataFrame from external storage systems
# MAGIC
# MAGIC **`spark.read.parquet("path/to/files")`**
# MAGIC
# MAGIC DataFrameReader is accessible through the SparkSession attribute **`read`**. This class includes methods to load DataFrames from different external storage systems.

# COMMAND ----------

# MAGIC %md ### Read from CSV files
# MAGIC Read from CSV with the DataFrameReader's **`csv`** method and the following options:
# MAGIC
# MAGIC Tab separator, use first line as header, infer schema

# COMMAND ----------

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .option("inferSchema", True)
           .csv(DA.paths.users_csv)
          )

users_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark's Python API also allows you to specify the DataFrameReader options as parameters to the **`csv`** method

# COMMAND ----------

users_df = (spark
           .read
           .csv(DA.paths.users_csv, sep="\t", header=True, inferSchema=True)
          )

users_df.printSchema()

# COMMAND ----------

# MAGIC %md Manually define the schema by creating a **`StructType`** with column names and data types

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("user_first_touch_timestamp", LongType(), True),
    StructField("email", StringType(), True)
])

# COMMAND ----------

# MAGIC %md Read from CSV using this user-defined schema instead of inferring the schema

# COMMAND ----------

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .schema(user_defined_schema)
           .csv(DA.paths.users_csv)
          )

# COMMAND ----------

# MAGIC %md Alternatively, define the schema using <a href="https://en.wikipedia.org/wiki/Data_definition_language" target="_blank">data definition language (DDL)</a> syntax.

# COMMAND ----------

ddl_schema = "user_id string, user_first_touch_timestamp long, email string"

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .schema(ddl_schema)
           .csv(DA.paths.users_csv)
          )

# COMMAND ----------

# MAGIC %md ### Read from JSON files
# MAGIC
# MAGIC Read from JSON with DataFrameReader's **`json`** method and the infer schema option

# COMMAND ----------

events_df = (spark
            .read
            .option("inferSchema", True)
            .json(DA.paths.events_json)
           )

events_df.printSchema()

# COMMAND ----------

# MAGIC %md Read data faster by creating a **`StructType`** with the schema names and data types

# COMMAND ----------

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField("device", StringType(), True),
    StructField("ecommerce", StructType([
        StructField("purchaseRevenue", DoubleType(), True),
        StructField("total_item_quantity", LongType(), True),
        StructField("unique_items", LongType(), True)
    ]), True),
    StructField("event_name", StringType(), True),
    StructField("event_previous_timestamp", LongType(), True),
    StructField("event_timestamp", LongType(), True),
    StructField("geo", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True),
    StructField("items", ArrayType(
        StructType([
            StructField("coupon", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("item_revenue_in_usd", DoubleType(), True),
            StructField("price_in_usd", DoubleType(), True),
            StructField("quantity", LongType(), True)
        ])
    ), True),
    StructField("traffic_source", StringType(), True),
    StructField("user_first_touch_timestamp", LongType(), True),
    StructField("user_id", StringType(), True)
])

events_df = (spark
            .read
            .schema(user_defined_schema)
            .json(DA.paths.events_json)
           )

# COMMAND ----------

# MAGIC %md You can use the **`StructType`** Scala method **`toDDL`** to have a DDL-formatted string created for you.
# MAGIC
# MAGIC This is convenient when you need to get the DDL-formated string for ingesting CSV and JSON but you don't want to hand craft it or the **`StructType`** variant of the schema.
# MAGIC
# MAGIC However, this functionality is not available in Python but the power of the notebooks allows us to use both languages.

# COMMAND ----------

# Step 1 - use this trick to transfer a value (the dataset path) between Python and Scala using the shared spark-config
spark.conf.set("com.whatever.your_scope.events_path", DA.paths.events_json)

# COMMAND ----------

# MAGIC %md
# MAGIC In a Python notebook like this one, create a Scala cell to injest the data and produce the DDL formatted schema

# COMMAND ----------

# MAGIC %scala
# MAGIC // Step 2 - pull the value from the config (or copy & paste it)
# MAGIC val eventsJsonPath = spark.conf.get("com.whatever.your_scope.events_path")
# MAGIC
# MAGIC // Step 3 - Read in the JSON, but let it infer the schema
# MAGIC val eventsSchema = spark.read
# MAGIC                         .option("inferSchema", true)
# MAGIC                         .json(eventsJsonPath)
# MAGIC                         .schema.toDDL
# MAGIC
# MAGIC // Step 4 - print the schema, select it, and copy it.
# MAGIC println("="*80)
# MAGIC println(eventsSchema)
# MAGIC println("="*80)

# COMMAND ----------

# Step 5 - paste the schema from above and assign it to a variable as seen here
events_schema = "`device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING"

# Step 6 - Read in the JSON data using our new DDL formatted string
events_df = (spark.read
                 .schema(events_schema)
                 .json(DA.paths.events_json))

display(events_df)

# COMMAND ----------

# MAGIC %md
# MAGIC This is a great "trick" for producing a schema for a net-new dataset and for accelerating development.
# MAGIC
# MAGIC When you are done (e.g. for Step #7), make sure to delete your temporary code.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png"> WARNING: **Do not use this trick in production**</br>
# MAGIC the inference of a schema can be REALLY slow as it<br/>
# MAGIC forces a full read of the source dataset to infer the schema

# COMMAND ----------

# MAGIC %md ## DataFrameWriter
# MAGIC Interface used to write a DataFrame to external storage systems
# MAGIC
# MAGIC <strong><code>
# MAGIC (df  
# MAGIC &nbsp;  .write                         
# MAGIC &nbsp;  .option("compression", "snappy")  
# MAGIC &nbsp;  .mode("overwrite")      
# MAGIC &nbsp;  .parquet(output_dir)       
# MAGIC )
# MAGIC </code></strong>
# MAGIC
# MAGIC DataFrameWriter is accessible through the SparkSession attribute **`write`**. This class includes methods to write DataFrames to different external storage systems.

# COMMAND ----------

# MAGIC %md ### Write DataFrames to files
# MAGIC
# MAGIC Write **`users_df`** to parquet with DataFrameWriter's **`parquet`** method and the following configurations:
# MAGIC
# MAGIC Snappy compression, overwrite mode

# COMMAND ----------

users_output_dir = DA.paths.working_dir + "/users.parquet"

(users_df
 .write
 .option("compression", "snappy")
 .mode("overwrite")
 .parquet(users_output_dir)
)

# COMMAND ----------

display(
    dbutils.fs.ls(users_output_dir)
)

# COMMAND ----------

# MAGIC %md
# MAGIC As with DataFrameReader, Spark's Python API also allows you to specify the DataFrameWriter options as parameters to the **`parquet`** method

# COMMAND ----------

(users_df
 .write
 .parquet(users_output_dir, compression="snappy", mode="overwrite")
)

# COMMAND ----------

# MAGIC %md ### Write DataFrames to tables
# MAGIC
# MAGIC Write **`events_df`** to a table using the DataFrameWriter method **`saveAsTable`**
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> This creates a global table, unlike the local view created by the DataFrame method **`createOrReplaceTempView`**

# COMMAND ----------

events_df.write.mode("overwrite").saveAsTable("events")

# COMMAND ----------

# MAGIC %md This table was saved in the database created for you in classroom setup. See database name printed below.

# COMMAND ----------

print(DA.schema_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake
# MAGIC
# MAGIC In almost all cases, the best practice is to use Delta Lake format, especially whenever the data will be referenced from a Databricks workspace. 
# MAGIC
# MAGIC <a href="https://delta.io/" target="_blank">Delta Lake</a> is an open source technology designed to work with Spark to bring reliability to data lakes.
# MAGIC
# MAGIC ![delta](https://files.training.databricks.com/images/aspwd/delta_storage_layer.png)
# MAGIC
# MAGIC #### Delta Lake's Key Features
# MAGIC - ACID transactions
# MAGIC - Scalable metadata handling
# MAGIC - Unified streaming and batch processing
# MAGIC - Time travel (data versioning)
# MAGIC - Schema enforcement and evolution
# MAGIC - Audit history
# MAGIC - Parquet format
# MAGIC - Compatible with Apache Spark API

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Results to a Delta Table
# MAGIC
# MAGIC Write **`events_df`** with the DataFrameWriter's **`save`** method and the following configurations: Delta format & overwrite mode.

# COMMAND ----------

events_output_path = DA.paths.working_dir + "/delta/events"

(events_df
 .write
 .format("delta")
 .mode("overwrite")
 .save(events_output_path)
)

# COMMAND ----------

# MAGIC %md ### Clean up classroom

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>