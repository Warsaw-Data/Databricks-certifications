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
-- MAGIC # Schemas and Tables on Databricks
-- MAGIC In this demonstration, you will create and explore schemas and tables.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Use Spark SQL DDL to define schemas and tables
-- MAGIC * Describe how the **`LOCATION`** keyword impacts the default storage directory
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Schemas and Tables - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Managed and Unmanaged Tables</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Creating a Table with the UI</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Create a Local Table</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Saving to Persistent Tables</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Lesson Setup
-- MAGIC The following script clears out previous runs of this demo and configures some Hive variables that will be used in our SQL queries.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## Schemas
-- MAGIC Let's start by creating a schema (database).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_default_location;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC  
-- MAGIC Note that the location of the first schema (database) is in the default location under **`dbfs:/user/hive/warehouse/`** and that the schema directory is the name of the schema with the **`.db`** extension

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED ${da.schema_name}_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managed Tables
-- MAGIC
-- MAGIC We will create a **managed** table (by not specifying a path for the location).
-- MAGIC
-- MAGIC We will create the table in the schema (database) we created above.
-- MAGIC
-- MAGIC Note that the table schema must be defined because there is no data from which to infer the table's columns and data types

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT);
INSERT INTO managed_table 
VALUES (3, 2, 1);
SELECT * FROM managed_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC We can look at the extended table description to find the location (you'll need to scroll down in the results).

-- COMMAND ----------

DESCRIBE DETAIL managed_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC By default, **managed** tables in a schema without the location specified will be created in the **`dbfs:/user/hive/warehouse/<schema_name>.db/`** directory.
-- MAGIC
-- MAGIC We can see that, as expected, the data and metadata for our table are stored in that location.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_location = spark.sql(f"DESCRIBE DETAIL managed_table").first().location
-- MAGIC print(tbl_location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Drop the table.

-- COMMAND ----------

DROP TABLE managed_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Note the table's directory and its log and data files are deleted. Only the schema (database) directory remains.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC schema_default_location = spark.sql(f"DESCRIBE SCHEMA {DA.schema_name}_default_location").collect()[3].database_description_value
-- MAGIC print(schema_default_location)
-- MAGIC dbutils.fs.ls(schema_default_location)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## External Tables
-- MAGIC Next, we will create an **external** (unmanaged) table from sample data. 
-- MAGIC
-- MAGIC The data we are going to use are in CSV format. We want to create a Delta table with a **`LOCATION`** provided in the directory of our choice.

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Let's note the location of the table's data in this lesson's working directory.

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Now, we drop the table.

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC The table definition no longer exists in the metastore, but the underlying data remain intact.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Clean up
-- MAGIC Drop the schema.

-- COMMAND ----------

DROP SCHEMA ${da.schema_name}_default_location CASCADE;

-- COMMAND ----------

-- MAGIC %md
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