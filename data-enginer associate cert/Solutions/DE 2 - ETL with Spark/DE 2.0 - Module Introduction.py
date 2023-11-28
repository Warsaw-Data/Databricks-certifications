# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Data with Spark SQL
# MAGIC This module is part of the Data Engineer Learning Path by Databricks Academy, and can be taken in SQL or Python.
# MAGIC
# MAGIC #### Extracting Data
# MAGIC These notebooks demonstrate Spark SQL concepts relevant to both SQL and PySpark users.  
# MAGIC
# MAGIC [DE 2.1 - Querying Files Directly]($./DE 2.1 - Querying Files Directly)  
# MAGIC [DE 2.2 - Providing Options for External Sources]($./DE 2.2 - Providing Options for External Sources)  
# MAGIC [DE 2.3L - Extract Data Lab]($./DE 2.3L - Extract Data Lab)
# MAGIC
# MAGIC #### Transforming Data
# MAGIC These notebooks include both Spark SQL queries and PySpark DataFrame code side by side to demonstrate the same concepts in both languages.
# MAGIC
# MAGIC [DE 2.4 - Cleaning Data]($./DE 2.4 - Cleaning Data)  
# MAGIC [DE 2.5 - Complex Transformations]($./DE 2.5 - Complex Transformations)  
# MAGIC [DE 2.6L - Reshape Data Lab]($./DE 2.6L - Reshape Data Lab)
# MAGIC
# MAGIC #### Additional Functions
# MAGIC
# MAGIC [DE 2.7A - SQL UDFs]($./DE 2.7A - SQL UDFs)  
# MAGIC [DE 2.7B - Python UDFs]($./DE 2.7B - Python UDFs)  
# MAGIC [DE 2.99 - OPTIONAL Higher Order Functions]($./DE 2.99 - OPTIONAL Higher Order Functions)  
# MAGIC
# MAGIC ### Prerequisites
# MAGIC Prerequisites for both versions of this course (Spark SQL and PySpark):
# MAGIC * Beginner familiarity with basic cloud concepts (virtual machines, object storage, identity management)
# MAGIC * Ability to perform basic code development tasks using the Databricks Data Engineering & Data Science workspace (create clusters, run code in notebooks, use basic notebook operations, import repos from git, etc)
# MAGIC * Intermediate familiarity with basic SQL concepts (select, filter, groupby, join, etc)
# MAGIC
# MAGIC Additional prerequisites for the PySpark version of this course:
# MAGIC * Beginner programming experience with Python (syntax, conditions, loops, functions)
# MAGIC * Beginner programming experience with the Spark DataFrame API:
# MAGIC * Configure DataFrameReader and DataFrameWriter to read and write data
# MAGIC * Express query transformations using DataFrame methods and Column expressions
# MAGIC * Navigate the Spark documentation to identify built-in functions for various transformations and data types
# MAGIC
# MAGIC Students can take the Introduction to PySpark Programming course from Databricks Academy to learn prerequisite skills for programming with the Spark DataFrame API. <br>
# MAGIC
# MAGIC
# MAGIC #### Technical Considerations
# MAGIC * This course runs on DBR 11.3.
# MAGIC * This course cannot be delivered on Databricks Community Edition.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>