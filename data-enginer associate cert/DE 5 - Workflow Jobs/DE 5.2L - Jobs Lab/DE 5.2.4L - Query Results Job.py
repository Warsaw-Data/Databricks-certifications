# Databricks notebook source
# MAGIC %run ../Includes/Classroom-Setup-05.2.4L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Exploring the Results of a DLT Pipeline
# MAGIC
# MAGIC Run the following cell to enumerate the output of your storage location:

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC The **system** directory captures events associated with the pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC These event logs are stored as a Delta table. 
# MAGIC
# MAGIC Let's query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${DA.paths.storage_location}/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Let's view the contents of the *tables* directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Let's query the gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${DA.schema_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()
