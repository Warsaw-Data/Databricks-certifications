# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks SQL
# MAGIC This module is part of the Data Engineer Learning Path by Databricks Academy.
# MAGIC
# MAGIC #### Lessons
# MAGIC [DE 7.1 - Navigating Databricks SQL and Attaching to Warehouses]($./DE 7.1 - Navigating Databricks SQL and Attaching to Warehouses) <br>
# MAGIC [DE 7.2 - Last Mile ETL with DBSQL]($./DE 7.2 - Last Mile ETL with DBSQL) <br>
# MAGIC
# MAGIC #### Prerequisites
# MAGIC * Ability to perform basic code development tasks using the Databricks Data Engineering & Data Science workspace (create clusters, run code in notebooks, use basic notebook operations, import repos from git, etc)
# MAGIC * Ability to configure and run data pipelines using the Delta Live Tables UI
# MAGIC * Beginner experience defining Delta Live Tables (DLT) pipelines using PySpark
# MAGIC * Ingest and process data using Auto Loader and PySpark syntax
# MAGIC * Process Change Data Capture feeds with APPLY CHANGES INTO syntax
# MAGIC * Review pipeline event logs and results to troubleshoot DLT syntax
# MAGIC * Production experience working with data warehouses and data lakes
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