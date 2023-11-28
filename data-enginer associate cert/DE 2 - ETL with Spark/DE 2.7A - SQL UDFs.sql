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
-- MAGIC # SQL UDFs and Control Flow
-- MAGIC
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Define and registering SQL UDFs
-- MAGIC * Describe the security model used for sharing SQL UDFs
-- MAGIC * Use **`CASE`** / **`WHEN`** statements in SQL code
-- MAGIC * Leverage **`CASE`** / **`WHEN`** statements in SQL UDFs for custom control flow

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Run Setup
-- MAGIC Run the following cell to setup your environment.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.7A

-- COMMAND ----------

-- MAGIC %md ## User-Defined Functions
-- MAGIC
-- MAGIC User Defined Functions (UDFs) in Spark SQL allow you to register custom SQL logic as functions in a database, making these methods reusable anywhere SQL can be run on Databricks. These functions are registered natively in SQL and maintain all of the optimizations of Spark when applying custom logic to large datasets.
-- MAGIC
-- MAGIC At minimum, creating a SQL UDF requires a function name, optional parameters, the type to be returned, and some custom logic.
-- MAGIC
-- MAGIC Below, a simple function named **`sale_announcement`** takes an **`item_name`** and **`item_price`** as parameters. It returns a string that announces a sale for an item at 80% of its original price.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT)
RETURNS STRING
RETURN concat("The ", item_name, " is on sale for $", round(item_price * 0.8, 0));

SELECT *, sale_announcement(name, price) AS message FROM item_lookup

-- COMMAND ----------

-- MAGIC %md Note that this function is applied to all values of the column in a parallel fashion within the Spark processing engine. SQL UDFs are an efficient way to define custom logic that is optimized for execution on Databricks.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Scoping and Permissions of SQL UDFs
-- MAGIC SQL user-defined functions:
-- MAGIC - Persist between execution environments (which can include notebooks, DBSQL queries, and jobs).
-- MAGIC - Exist as objects in the metastore and are governed by the same Table ACLs as databases, tables, or views.
-- MAGIC - To **create** a SQL UDF, you need **`USE CATALOG`** on the catalog, and **`USE SCHEMA`** and **`CREATE FUNCTION`** on the schema.
-- MAGIC - To **use** a SQL UDF, you need **`USE CATALOG`** on the catalog, **`USE SCHEMA`** on the schema, and **`EXECUTE`** on the function.
-- MAGIC
-- MAGIC We can use **`DESCRIBE FUNCTION`** to see where a function was registered and basic information about expected inputs and what is returned (and even more information with **`DESCRIBE FUNCTION EXTENDED`**).

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED sale_announcement

-- COMMAND ----------

-- MAGIC %md Note that the **`Body`** field at the bottom of the function description shows the SQL logic used in the function itself.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Simple Control Flow Functions
-- MAGIC
-- MAGIC Combining SQL UDFs with control flow in the form of **`CASE`** / **`WHEN`** clauses provides optimized execution for control flows within SQL workloads. The standard SQL syntactic construct **`CASE`** / **`WHEN`** allows the evaluation of multiple conditional statements with alternative outcomes based on table contents.
-- MAGIC
-- MAGIC Here, we demonstrate wrapping this control flow logic in a function that will be reusable anywhere we can execute SQL. 

-- COMMAND ----------

CREATE OR REPLACE FUNCTION item_preference(name STRING, price INT)
RETURNS STRING
RETURN CASE 
  WHEN name = "Standard Queen Mattress" THEN "This is my default mattress"
  WHEN name = "Premium Queen Mattress" THEN "This is my favorite mattress"
  WHEN price > 100 THEN concat("I'd wait until the ", name, " is on sale for $", round(price * 0.8, 0))
  ELSE concat("I don't need a ", name)
END;

SELECT *, item_preference(name, price) FROM item_lookup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC While the examples provided here are simple, these same basic principles can be used to add custom computations and logic for native execution in Spark SQL. 
-- MAGIC
-- MAGIC Especially for enterprises that might be migrating users from systems with many defined procedures or custom-defined formulas, SQL UDFs can allow a handful of users to define the complex logic needed for common reporting and analytic queries.

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