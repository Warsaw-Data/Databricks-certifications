# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Abandoned Carts Lab
# MAGIC Get abandoned cart items for email without purchases.
# MAGIC 1. Get emails of converted users from transactions
# MAGIC 2. Join emails with user IDs
# MAGIC 3. Get cart item history for each user
# MAGIC 4. Join cart item history with emails
# MAGIC 5. Filter for emails with abandoned cart items
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join" target="_blank">DataFrame</a>: **`join`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-In Functions</a>: **`collect_set`**, **`explode`**, **`lit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>: **`fill`**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Run the cells below to create DataFrames **`sales_df`**, **`users_df`**, and **`events_df`**.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.12L

# COMMAND ----------

# sale transactions at BedBricks
sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# user IDs and emails at BedBricks
users_df = spark.table("users")
display(users_df)

# COMMAND ----------

# events logged on the BedBricks website
events_df = spark.table("events")
display(events_df)

# COMMAND ----------

# MAGIC %md ### 1: Get emails of converted users from transactions
# MAGIC - Select the **`email`** column in **`sales_df`** and remove duplicates
# MAGIC - Add a new column **`converted`** with the value **`True`** for all rows
# MAGIC
# MAGIC Save the result as **`converted_users_df`**.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import *

converted_users_df = (sales_df
                      .select("email")
                      .distinct()
                      .withColumn("converted", lit(True))
                     )
display(converted_users_df)

# COMMAND ----------

# MAGIC %md #### 1.1: Check Your Work
# MAGIC
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["email", "converted"]

expected_count = 10510

assert converted_users_df.columns == expected_columns, "converted_users_df does not have the correct columns"

assert converted_users_df.count() == expected_count, "converted_users_df does not have the correct number of rows"

assert converted_users_df.select(col("converted")).first()[0] == True, "converted column not correct"
print("All test pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2: Join emails with user IDs
# MAGIC - Perform an outer join on **`converted_users_df`** and **`users_df`** with the **`email`** field
# MAGIC - Filter for users where **`email`** is not null
# MAGIC - Fill null values in **`converted`** as **`False`**
# MAGIC
# MAGIC Save the result as **`conversions_df`**.

# COMMAND ----------

# ANSWER
conversions_df = (users_df
                  .join(converted_users_df, "email", "outer")
                  .filter(col("email").isNotNull())
                  .na.fill(False)
                 )
display(conversions_df)

# COMMAND ----------

# MAGIC %md #### 2.1: Check Your Work
# MAGIC
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ['email', 'user_id', 'user_first_touch_timestamp', 'updated', 'converted']

expected_count = 38939

expected_false_count = 28429

assert conversions_df.columns == expected_columns, "Columns are not correct"

assert conversions_df.filter(col("email").isNull()).count() == 0, "Email column contains null"

assert conversions_df.count() == expected_count, "There is an incorrect number of rows"

assert conversions_df.filter(col("converted") == False).count() == expected_false_count, "There is an incorrect number of false entries in converted column"
print("All test pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3: Get cart item history for each user
# MAGIC - Explode the **`items`** field in **`events_df`** with the results replacing the existing **`items`** field
# MAGIC - Group by **`user_id`**
# MAGIC   - Collect a set of all **`items.item_id`** objects for each user and alias the column to "cart"
# MAGIC
# MAGIC Save the result as **`carts_df`**.

# COMMAND ----------

# ANSWER
carts_df = (events_df
            .withColumn("items", explode("items"))
            .groupBy("user_id").agg(collect_set("items.item_id").alias("cart"))
           )
display(carts_df)

# COMMAND ----------

# MAGIC %md #### 3.1: Check Your Work
# MAGIC
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["user_id", "cart"]

expected_count = 24574

assert carts_df.columns == expected_columns, "Incorrect columns"

assert carts_df.count() == expected_count, "Incorrect number of rows"

assert carts_df.select(col("user_id")).drop_duplicates().count() == expected_count, "Duplicate user_ids present"
print("All test pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4: Join cart item history with emails
# MAGIC - Perform a left join on **`conversions_df`** and **`carts_df`** on the **`user_id`** field
# MAGIC
# MAGIC Save result as **`email_carts_df`**.

# COMMAND ----------

# ANSWER
email_carts_df = conversions_df.join(carts_df, "user_id", "left")
display(email_carts_df)

# COMMAND ----------

# MAGIC %md #### 4.1: Check Your Work
# MAGIC
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

email_carts_df.filter(col("cart").isNull()).count()

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "updated", "converted", "cart"]

expected_count = 38939

expected_cart_null_count = 19671

assert email_carts_df.columns == expected_columns, "Columns do not match"

assert email_carts_df.count() == expected_count, "Counts do not match"

assert email_carts_df.filter(col("cart").isNull()).count() == expected_cart_null_count, "Cart null counts incorrect from join"
print("All test pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5: Filter for emails with abandoned cart items
# MAGIC - Filter **`email_carts_df`** for users where **`converted`** is False
# MAGIC - Filter for users with non-null carts
# MAGIC
# MAGIC Save result as **`abandoned_carts_df`**.

# COMMAND ----------

# ANSWER
abandoned_carts_df = (email_carts_df
                      .filter(col("converted") == False)
                      .filter(col("cart").isNotNull())
                     )
display(abandoned_carts_df)

# COMMAND ----------

# MAGIC %md #### 5.1: Check Your Work
# MAGIC
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "updated", "converted", "cart"]

expected_count = 10212

assert abandoned_carts_df.columns == expected_columns, "Columns do not match"

assert abandoned_carts_df.count() == expected_count, "Counts do not match"
print("All test pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6: Bonus Activity
# MAGIC Plot number of abandoned cart items by product

# COMMAND ----------

# ANSWER
abandoned_items_df = (abandoned_carts_df
                      .withColumn("items", explode("cart"))
                      .groupBy("items")
                      .count()
                      .sort("items")
                     )
display(abandoned_items_df)

# COMMAND ----------

# MAGIC %md #### 6.1: Check Your Work
# MAGIC
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["items", "count"]

expected_count = 12

assert abandoned_items_df.count() == expected_count, "Counts do not match"

assert abandoned_items_df.columns == expected_columns, "Columns do not match"
print("All test pass")

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