# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Land New Data
# MAGIC
# MAGIC This notebook is provided for the sole purpose of manually triggering new batches of data to be processed by an already configured Delta Live Tables pipeline.
# MAGIC
# MAGIC Note that the logic provided is identical to that provided in the first interactive notebook, but does not reset the source or target directories for the pipeline.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.99

# COMMAND ----------

# MAGIC %md
# MAGIC Each time this cell is run, a new batch of data files will be loaded into the source directory used in these lessons.

# COMMAND ----------

DA.dlt_data_factory.load()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Land All Remaining Data
# MAGIC Alternatively, you can uncomment and run the following cell to load all remaining batches of data.

# COMMAND ----------

# TODO
This should run for a little over 5 minutes. To abort
early, click the Stop Execution button above
DA.dlt_data_factory.load(continuous=True, delay_seconds=10)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>