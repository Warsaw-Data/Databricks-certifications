-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Higher Order Functions in Spark SQL
-- MAGIC
-- MAGIC Higher order functions in Spark SQL allow you to transform complex data types, such as array or map type objects, while preserving their original structures. Examples include:
-- MAGIC - **`FILTER()`** filters an array using the given lambda function.
-- MAGIC - **`EXIST()`** tests whether a statement is true for one or more elements in an array. 
-- MAGIC - **`TRANSFORM()`** uses the given lambda function to transform all elements in an array.
-- MAGIC - **`REDUCE()`** takes two lambda functions to reduce the elements of an array to a single value by merging the elements into a buffer, and the apply a finishing function on the final buffer. 
-- MAGIC
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Use higher order functions to work with arrays

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Run Setup
-- MAGIC Run the following cell to setup your environment.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.99

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Filter
-- MAGIC We can use the **`FILTER`** function to create a new column that excludes values from each array based on a provided condition.  
-- MAGIC Let's use this to remove products in the **`items`** column that are not king-sized from all records in our **`sales`** dataset. 
-- MAGIC
-- MAGIC **`FILTER (items, i -> i.item_id LIKE "%K") AS king_items`**
-- MAGIC
-- MAGIC In the statement above:
-- MAGIC - **`FILTER`** : the name of the higher-order function <br>
-- MAGIC - **`items`** : the name of our input array <br>
-- MAGIC - **`i`** : the name of the iterator variable. You choose this name and then use it in the lambda function. It iterates over the array, cycling each value into the function one at a time.<br>
-- MAGIC - **`->`** :  Indicates the start of a function <br>
-- MAGIC - **`i.item_id LIKE "%K"`** : This is the function. Each value is checked to see if it ends with the capital letter K. If it is, it gets filtered into the new column, **`king_items`**
-- MAGIC
-- MAGIC **NOTE:** You may write a filter that produces a lot of empty arrays in the created column. When that happens, it can be useful to use a **`WHERE`** clause to show only non-empty array values in the returned column. 

-- COMMAND ----------

SELECT * FROM (
  SELECT
    order_id,
    FILTER (items, i -> i.item_id LIKE "%K") AS king_items
  FROM sales)
WHERE size(king_items) > 0

-- COMMAND ----------

-- MAGIC %md ## Transform
-- MAGIC
-- MAGIC The **`TRANSFORM()`** higher order function can be particularly useful when you want to apply an existing function to each element in an array.  
-- MAGIC Let's apply this to create a new array column called **`item_revenues`** by transforming the elements contained in the **`items`** array column.
-- MAGIC
-- MAGIC In the query below: **`items`** is the name of our input array, **`i`** is the name of the iterator variable (you choose this name and then use it in the lambda function; it iterates over the array, cycling each value into the function one at a time), and  **`->`**  indicates the start of a function.  

-- COMMAND ----------

SELECT *,
  TRANSFORM (
    items, i -> CAST(i.item_revenue_in_usd * 100 AS INT)
  ) AS item_revenues
FROM sales

-- COMMAND ----------

-- MAGIC %md The lambda function we specified above takes the **`item_revenue_in_usd`** subfield of each value, multiplies that by 100, casts to integer, and includes the result in the new array column, **`item_revenues`**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Exists Lab
-- MAGIC Here, you'll use the higher order function **`EXISTS`** with data from the **`sales`** table to create boolean columns **`mattress`** and **`pillow`** that indicate whether the item purchased was a mattress or pillow product.
-- MAGIC
-- MAGIC For example, if **`item_name`** from the **`items`** column ends with the string **`"Mattress"`**, the column value for **`mattress`** should be **`true`** and the value for **`pillow`** should be **`false`**. Here are a few examples of items and the resulting values.
-- MAGIC
-- MAGIC |  items  | mattress | pillow |
-- MAGIC | ------- | -------- | ------ |
-- MAGIC | **`[{..., "item_id": "M_PREM_K", "item_name": "Premium King Mattress", ...}]`** | true | false |
-- MAGIC | **`[{..., "item_id": "P_FOAM_S", "item_name": "Standard Foam Pillow", ...}]`** | false | true |
-- MAGIC | **`[{..., "item_id": "M_STAN_F", "item_name": "Standard Full Mattress", ...}]`** | true | false |
-- MAGIC
-- MAGIC See documentation for the <a href="https://docs.databricks.com/sql/language-manual/functions/exists.html" target="_blank">exists</a> function.  
-- MAGIC You can use the condition expression **`item_name LIKE "%Mattress"`** to check whether the string **`item_name`** ends with the word "Mattress".

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE sales_product_flags AS
SELECT
  items,
  EXISTS (items, i -> i.item_name LIKE "%Mattress") AS mattress,
  EXISTS (items, i -> i.item_name LIKE "%Pillow") AS pillow
FROM sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC The helper function below will return an error with a message on what needs to change if you have not followed instructions. No output means that you have completed this step.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, num_rows, column_names):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert set(spark.table(table_name).columns) == set(column_names), "Please name the columns as shown in the schema above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Run the cell below to confirm the table was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("sales_product_flags", 10510, ['items', 'mattress', 'pillow'])
-- MAGIC product_counts = spark.sql("SELECT sum(CAST(mattress AS INT)) num_mattress, sum(CAST(pillow AS INT)) num_pillow FROM sales_product_flags").first().asDict()
-- MAGIC assert product_counts == {'num_mattress': 9986, 'num_pillow': 1384}, "There should be 9986 rows where mattress is true, and 1384 where pillow is true"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>