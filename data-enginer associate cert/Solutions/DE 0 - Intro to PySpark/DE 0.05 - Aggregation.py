# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregation
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Group data by specified columns
# MAGIC 1. Apply grouped data methods to aggregate data
# MAGIC 1. Apply built-in functions to aggregate data
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`groupBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank" target="_blank">Grouped Data</a>: **`agg`**, **`avg`**, **`count`**, **`max`**, **`sum`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-In Functions</a>: **`approx_count_distinct`**, **`avg`**, **`sum`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.05

# COMMAND ----------

# MAGIC %md Let's use the BedBricks events dataset.

# COMMAND ----------

df = spark.table("events")
display(df)

# COMMAND ----------

# MAGIC %md ### Grouping data
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/aspwd/aggregation_groupby.png" width="60%" />

# COMMAND ----------

# MAGIC %md ### groupBy
# MAGIC Use the DataFrame **`groupBy`** method to create a grouped data object. 
# MAGIC
# MAGIC This grouped data object is called **`RelationalGroupedDataset`** in Scala and **`GroupedData`** in Python.

# COMMAND ----------

df.groupBy("event_name")

# COMMAND ----------

df.groupBy("geo.state", "geo.city")

# COMMAND ----------

# MAGIC %md ### Grouped data methods
# MAGIC Various aggregation methods are available on the <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank">GroupedData</a> object.
# MAGIC
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | agg | Compute aggregates by specifying a series of aggregate columns |
# MAGIC | avg | Compute the mean value for each numeric columns for each group |
# MAGIC | count | Count the number of rows for each group |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | min | Compute the min value for each numeric column for each group |
# MAGIC | pivot | Pivots a column of the current DataFrame and performs the specified aggregation |
# MAGIC | sum | Compute the sum for each numeric columns for each group |

# COMMAND ----------

event_counts_df = df.groupBy("event_name").count()
display(event_counts_df)

# COMMAND ----------

# MAGIC %md Here, we're getting the average purchase revenue for each.

# COMMAND ----------

avg_state_purchases_df = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
display(avg_state_purchases_df)

# COMMAND ----------

# MAGIC %md
# MAGIC And here the total quantity and sum of the purchase revenue for each combination of state and city.

# COMMAND ----------

city_purchase_quantities_df = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity", "ecommerce.purchase_revenue_in_usd")
display(city_purchase_quantities_df)

# COMMAND ----------

# MAGIC %md ## Built-In Functions
# MAGIC In addition to DataFrame and Column transformation methods, there are a ton of helpful functions in Spark's built-in <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html" target="_blank">SQL functions</a> module.
# MAGIC
# MAGIC In Scala, this is <a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">**`org.apache.spark.sql.functions`**</a>, and <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions" target="_blank">**`pyspark.sql.functions`**</a> in Python. Functions from this module must be imported into your code.

# COMMAND ----------

# MAGIC %md ### Aggregate Functions
# MAGIC
# MAGIC Here are some of the built-in functions available for aggregation.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | approx_count_distinct | Returns the approximate number of distinct items in a group |
# MAGIC | avg | Returns the average of the values in a group |
# MAGIC | collect_list | Returns a list of objects with duplicates |
# MAGIC | corr | Returns the Pearson Correlation Coefficient for two columns |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | stddev_samp | Returns the sample standard deviation of the expression in a group |
# MAGIC | sumDistinct | Returns the sum of distinct values in the expression |
# MAGIC | var_pop | Returns the population variance of the values in a group |
# MAGIC
# MAGIC Use the grouped data method <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg" target="_blank">**`agg`**</a> to apply built-in aggregate functions
# MAGIC
# MAGIC This allows you to apply other transformations on the resulting columns, such as <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html" target="_blank">**`alias`**</a>.

# COMMAND ----------

from pyspark.sql.functions import sum

state_purchases_df = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
display(state_purchases_df)

# COMMAND ----------

# MAGIC %md Apply multiple aggregate functions on grouped data

# COMMAND ----------

from pyspark.sql.functions import avg, approx_count_distinct

state_aggregates_df = (df
                       .groupBy("geo.state")
                       .agg(avg("ecommerce.total_item_quantity").alias("avg_quantity"),
                            approx_count_distinct("user_id").alias("distinct_users"))
                      )

display(state_aggregates_df)

# COMMAND ----------

# MAGIC %md ### Math Functions
# MAGIC Here are some of the built-in functions for math operations.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | ceil | Computes the ceiling of the given column. |
# MAGIC | cos | Computes the cosine of the given value. |
# MAGIC | log | Computes the natural logarithm of the given value. |
# MAGIC | round | Returns the value of the column e rounded to 0 decimal places with HALF_UP round mode. |
# MAGIC | sqrt | Computes the square root of the specified float value. |

# COMMAND ----------

from pyspark.sql.functions import cos, sqrt

display(spark.range(10)  # Create a DataFrame with a single column called "id" with a range of integer values
        .withColumn("sqrt", sqrt("id"))
        .withColumn("cos", cos("id"))
       )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>