# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Complex Types
# MAGIC
# MAGIC Explore built-in functions for working with collections and strings.
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Apply collection functions to process arrays
# MAGIC 1. Union DataFrames together
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>:**`union`**, **`unionByName`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-In Functions</a>:
# MAGIC   - Aggregate: **`collect_set`**
# MAGIC   - Collection: **`array_contains`**, **`element_at`**, **`explode`**
# MAGIC   - String: **`split`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.10

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("sales")

display(df)

# COMMAND ----------

# You will need this DataFrame for a later exercise
details_df = (df
              .withColumn("items", explode("items"))
              .select("email", "items.item_name")
              .withColumn("details", split(col("item_name"), " "))
             )
display(details_df)

# COMMAND ----------

# MAGIC %md ### String Functions
# MAGIC Here are some of the built-in functions available for manipulating strings.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | translate | Translate any character in the src by a character in replaceString |
# MAGIC | regexp_replace | Replace all substrings of the specified string value that match regexp with rep |
# MAGIC | regexp_extract | Extract a specific group matched by a Java regex, from the specified string column |
# MAGIC | ltrim | Removes the leading space characters from the specified string column |
# MAGIC | lower | Converts a string column to lowercase |
# MAGIC | split | Splits str around matches of the given pattern |

# COMMAND ----------

# MAGIC %md
# MAGIC For example: let's imagine that we need to parse our **`email`** column. We're going to use the **`split`** function  to split domain and handle.

# COMMAND ----------

from pyspark.sql.functions import split

# COMMAND ----------

display(df.select(split(df.email, '@', 0).alias('email_handle')))

# COMMAND ----------

# MAGIC %md ### Collection Functions
# MAGIC
# MAGIC Here are some of the built-in functions available for working with arrays.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | array_contains | Returns null if the array is null, true if the array contains value, and false otherwise. |
# MAGIC | element_at | Returns element of array at given index. Array elements are numbered starting with **1**. |
# MAGIC | explode | Creates a new row for each element in the given array or map column. |
# MAGIC | collect_set | Returns a set of objects with duplicate elements eliminated. |

# COMMAND ----------

mattress_df = (details_df
               .filter(array_contains(col("details"), "Mattress"))
               .withColumn("size", element_at(col("details"), 2)))
display(mattress_df)

# COMMAND ----------

# MAGIC %md ### Aggregate Functions
# MAGIC
# MAGIC Here are some of the built-in aggregate functions available for creating arrays, typically from GroupedData.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | collect_list | Returns an array consisting of all values within the group. |
# MAGIC | collect_set | Returns an array consisting of all unique values within the group. |

# COMMAND ----------

# MAGIC %md Let's say that we wanted to see the sizes of mattresses ordered by each email address. For this, we can use the **`collect_set`** function

# COMMAND ----------

size_df = mattress_df.groupBy("email").agg(collect_set("size").alias("size options"))

display(size_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Union and unionByName
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> The DataFrame <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.union.html" target="_blank">**`union`**</a> method resolves columns by position, as in standard SQL. You should use it only if the two DataFrames have exactly the same schema, including the column order. In contrast, the DataFrame <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.unionByName.html" target="_blank">**`unionByName`**</a> method resolves columns by name.  This is equivalent to UNION ALL in SQL.  Neither one will remove duplicates.  
# MAGIC
# MAGIC Below is a check to see if the two dataframes have a matching schema where **`union`** would be appropriate

# COMMAND ----------

mattress_df.schema==size_df.schema

# COMMAND ----------

# MAGIC %md 
# MAGIC If we do get the two schemas to match with a simple **`select`** statement, then we can use a **`union`**

# COMMAND ----------

union_count = mattress_df.select("email").union(size_df.select("email")).count()

mattress_count = mattress_df.count()
size_count = size_df.count()

mattress_count + size_count == union_count

# COMMAND ----------

# MAGIC %md ## Clean up classroom
# MAGIC
# MAGIC And lastly, we'll clean up the classroom.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>