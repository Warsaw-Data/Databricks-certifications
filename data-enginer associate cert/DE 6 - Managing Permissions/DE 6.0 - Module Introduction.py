# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Managing Data Access for Analytics
# MAGIC This module is part of the Data Engineer Learning Path by Databricks Academy.
# MAGIC
# MAGIC #### Lessons
# MAGIC Slides: Introduction to Unity Catalog<br>
# MAGIC DE 6.1 - [Create and Govern Data with UC]($./DE 6.1 - Create and Govern Data with UC)<br>
# MAGIC DE 6.2 - [Create and Share Tables in Unity Catalog]($./DE 6.2 - Create and Share Tables in Unity Catalog)<br>
# MAGIC DE 6.3 - [Create Views and Limit Table Access]($./DE 6.3 - Create Views and Limit Table Access)<br>
# MAGIC
# MAGIC
# MAGIC #### Administration with Unity Catalog - OPTIONAL
# MAGIC DE 6.99 - OPTIONAL Administration<br>
# MAGIC DE 6.99.2 - [Create compute resources for Unity Catalog access]($./DE 6.99 - OPTIONAL Administration/DE 6.99.2 - Create compute resources for Unity Catalog access)<br>
# MAGIC DE 6.99.3 - [Upgrade a Table to Unity Catalog]($./DE 6.99 - OPTIONAL Administration/DE 6.99.3 - OPTIONAL Upgrade a Table to Unity Catalog)<br>
# MAGIC
# MAGIC
# MAGIC #### Prerequisites
# MAGIC * Beginning-level knowledge of the Databricks Lakehouse platform (high-level knowledge the structure and benefits of the Lakehouse platform)
# MAGIC * Beginning-level knowledge of SQL (ability to understand and construct basic queries)
# MAGIC
# MAGIC
# MAGIC #### Technical Considerations
# MAGIC This course cannot be delivered on Databricks Community Edition, and can only be delivered on clouds supporting Unity Catalog. Administration access at both the workspace and account level is required to fully perform all exercises. A few optional tasks are demonstrated that additionally require low-level access to the cloud environment.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>