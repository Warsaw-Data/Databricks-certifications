-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # More DLT SQL Syntax
-- MAGIC
-- MAGIC DLT Pipelines make it easy to combine multiple datasets into a single scalable workload using one or many notebooks.
-- MAGIC
-- MAGIC In the last notebook, we reviewed some of the basic functionality of DLT syntax while processing data from cloud object storage through a series of queries to validate and enrich records at each step. This notebook similarly follows the medallion architecture, but introduces a number of new concepts.
-- MAGIC * Raw records represent change data capture (CDC) information about customers 
-- MAGIC * The bronze table again uses Auto Loader to ingest JSON data from cloud object storage
-- MAGIC * A table is defined to enforce constraints before passing records to the silver layer
-- MAGIC * **`APPLY CHANGES INTO`** is used to automatically process CDC data into the silver layer as a Type 1 <a href="https://en.wikipedia.org/wiki/Slowly_changing_dimension" target="_blank">slowly changing dimension (SCD) table<a/>
-- MAGIC * A gold table is defined to calculate an aggregate from the current version of this Type 1 table
-- MAGIC * A view is defined that joins with tables defined in another notebook
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, students should feel comfortable:
-- MAGIC * Processing CDC data with **`APPLY CHANGES INTO`**
-- MAGIC * Declaring live views
-- MAGIC * Joining live tables
-- MAGIC * Describing how DLT library notebooks work together in a pipeline
-- MAGIC * Scheduling multiple notebooks in a DLT pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingest Data with Auto Loader
-- MAGIC
-- MAGIC As in the last notebook, we define a bronze table against a data source configured with Auto Loader.
-- MAGIC
-- MAGIC Note that the code below omits the Auto Loader option to infer schema. When data is ingested from JSON without the schema provided or inferred, fields will have the correct names but will all be stored as **`STRING`** type.
-- MAGIC
-- MAGIC The code below also provides a simple comment and adds fields for time of data ingestion and the file name for each record.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers_bronze
COMMENT "Raw data from customers CDC feed"
AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
FROM cloud_files("${source}/customers", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Quality Enforcement Continued
-- MAGIC
-- MAGIC The query below demonstrates:
-- MAGIC * The 3 options for behavior when constraints are violated
-- MAGIC * A query with multiple constraints
-- MAGIC * Multiple conditions provided to one constraint
-- MAGIC * Using a built-in SQL function in a constraint
-- MAGIC
-- MAGIC About the data source:
-- MAGIC * Data is a CDC feed that contains **`INSERT`**, **`UPDATE`**, and **`DELETE`** operations. 
-- MAGIC * Update and insert operations should contain valid entries for all fields.
-- MAGIC * Delete operations should contain **`NULL`** values for all fields other than the timestamp, **`customer_id`**, and operation fields.
-- MAGIC
-- MAGIC In order to ensure only good data makes it into our silver table, we'll write a series of quality enforcement rules that ignore the expected null values in delete operations.
-- MAGIC
-- MAGIC We'll break down each of these constraints below:
-- MAGIC
-- MAGIC ##### **`valid_id`**
-- MAGIC This constraint will cause our transaction to fail if a record contains a null value in the **`customer_id`** field.
-- MAGIC
-- MAGIC ##### **`valid_operation`**
-- MAGIC This contraint will drop any records that contain a null value in the **`operation`** field.
-- MAGIC
-- MAGIC ##### **`valid_address`**
-- MAGIC This constraint checks if the **`operation`** field is **`DELETE`**; if not, it checks for null values in any of the 4 fields comprising an address. Because there is no additional instruction for what to do with invalid records, violating rows will be recorded in metrics but not dropped.
-- MAGIC
-- MAGIC ##### **`valid_email`**
-- MAGIC This constraint uses regex pattern matching to check that the value in the **`email`** field is a valid email address. It contains logic to not apply this to records if the **`operation`** field is **`DELETE`** (because these will have a null value for the **`email`** field). Violating records are dropped.

-- COMMAND ----------

CREATE STREAMING LIVE TABLE customers_bronze_clean
(CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_name EXPECT (name IS NOT NULL or operation = "DELETE"),
CONSTRAINT valid_address EXPECT (
  (address IS NOT NULL and 
  city IS NOT NULL and 
  state IS NOT NULL and 
  zip_code IS NOT NULL) or
  operation = "DELETE"),
CONSTRAINT valid_email EXPECT (
  rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') or 
  operation = "DELETE") ON VIOLATION DROP ROW)
AS SELECT *
  FROM STREAM(LIVE.customers_bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Processing CDC Data with **`APPLY CHANGES INTO`**
-- MAGIC
-- MAGIC DLT introduces a new syntactic structure for simplifying CDC feed processing.
-- MAGIC
-- MAGIC **`APPLY CHANGES INTO`** has the following guarantees and requirements:
-- MAGIC * Performs incremental/streaming ingestion of CDC data
-- MAGIC * Provides simple syntax to specify one or many fields as the primary key for a table
-- MAGIC * Default assumption is that rows will contain inserts and updates
-- MAGIC * Can optionally apply deletes
-- MAGIC * Automatically orders late-arriving records using user-provided sequencing key
-- MAGIC * Uses a simple syntax for specifying columns to ignore with the **`EXCEPT`** keyword
-- MAGIC * Will default to applying changes as Type 1 SCD
-- MAGIC
-- MAGIC The code below:
-- MAGIC * Creates the **`customers_silver`** table; **`APPLY CHANGES INTO`** requires the target table to be declared in a separate statement
-- MAGIC * Identifies the **`customers_silver`** table as the target into which the changes will be applied
-- MAGIC * Specifies the table **`customers_bronze_clean`** as the streaming source
-- MAGIC * Identifies the **`customer_id`** as the primary key
-- MAGIC * Specifies that records where the **`operation`** field is **`DELETE`** should be applied as deletes
-- MAGIC * Specifies the **`timestamp`** field for ordering how operations should be applied
-- MAGIC * Indicates that all fields should be added to the target table except **`operation`**, **`source_file`**, and **`_rescued_data`**

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers_silver;

APPLY CHANGES INTO LIVE.customers_silver
  FROM STREAM(LIVE.customers_bronze_clean)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY timestamp
  COLUMNS * EXCEPT (operation, source_file, _rescued_data)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying Tables with Applied Changes
-- MAGIC
-- MAGIC **`APPLY CHANGES INTO`** defaults to creating a Type 1 SCD table, meaning that each unique key will have at most 1 record and that updates will overwrite the original information.
-- MAGIC
-- MAGIC While the target of our operation in the previous cell was defined as a streaming live table, data is being updated and deleted in this table (and so breaks the append-only requirements for streaming live table sources). As such, downstream operations cannot perform streaming queries against this table. 
-- MAGIC
-- MAGIC This pattern ensures that if any updates arrive out of order, downstream results can be properly recomputed to reflect updates. It also ensures that when records are deleted from a source table, these values are no longer reflected in tables later in the pipeline.
-- MAGIC
-- MAGIC Below, we define a simple aggregate query to create a live table from the data in the **`customers_silver`** table.

-- COMMAND ----------

CREATE LIVE TABLE customer_counts_state
  COMMENT "Total active customers per state"
AS SELECT state, count(*) as customer_count, current_timestamp() updated_at
  FROM LIVE.customers_silver
  GROUP BY state

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DLT Views
-- MAGIC
-- MAGIC The query below defines a DLT view by replacing **`TABLE`** with the **`VIEW`** keyword.
-- MAGIC
-- MAGIC Views in DLT differ from persisted tables, and can optionally be defined as **`STREAMING`**.
-- MAGIC
-- MAGIC Views have the same update guarantees as live tables, but the results of queries are not stored to disk.
-- MAGIC
-- MAGIC Unlike views used elsewhere in Databricks, DLT views are not persisted to the metastore, meaning that they can only be referenced from within the DLT pipeline they are a part of. (This is similar scoping to temporary views in most SQL systems.)
-- MAGIC
-- MAGIC Views can still be used to enforce data quality, and metrics for views will be collected and reported as they would be for tables.
-- MAGIC
-- MAGIC ## Joins and Referencing Tables Across Notebook Libraries
-- MAGIC
-- MAGIC The code we've reviewed thus far has shown 2 source datasets propagating through a series of steps in separate notebooks.
-- MAGIC
-- MAGIC DLT supports scheduling multiple notebooks as part of a single DLT Pipeline configuration. You can edit existing DLT pipelines to add additional notebooks.
-- MAGIC
-- MAGIC Within a DLT Pipeline, code in any notebook library can reference tables and views created in any other notebook library.
-- MAGIC
-- MAGIC Essentially, we can think of the scope of the schema reference by the **`LIVE`** keyword to be at the DLT Pipeline level, rather than the individual notebook.
-- MAGIC
-- MAGIC In the query below, we create a new view by joining the silver tables from our **`orders`** and **`customers`** datasets. Note that this view is not defined as streaming; as such, we will always capture the current valid **`email`** for each customer, and will automatically drop records for customers after they've been deleted from the **`customers_silver`** table.

-- COMMAND ----------

CREATE LIVE VIEW subscribed_order_emails_v
  AS SELECT a.customer_id, a.order_id, b.email 
    FROM LIVE.orders_silver a
    INNER JOIN LIVE.customers_silver b
    ON a.customer_id = b.customer_id
    WHERE notifications = 'Y'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Adding this Notebook to a DLT Pipeline
-- MAGIC
-- MAGIC Adding additional notebook libraries to an existing pipeline is accomplished easily with the DLT UI.
-- MAGIC
-- MAGIC 1. Navigate to the DLT Pipeline you configured earlier in the course
-- MAGIC 1. Click the **Settings** button in the top right
-- MAGIC 1. Under **Notebook Libraries**, click **Add notebook library**
-- MAGIC    * Use the file picker to select this notebook, then click **Select**
-- MAGIC 1. Click the **Save** button to save your updates
-- MAGIC 1. Click the blue **Start** button in the top right of the screen to update your pipeline and process any new records
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> The link to this notebook can be found back in [DE 4.1 - DLT UI Walkthrough]($../DE 4.1 - DLT UI Walkthrough)<br/>
-- MAGIC in the printed instructions for **Task #2** under the section **Generate Pipeline Configuration**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary
-- MAGIC
-- MAGIC By reviewing this notebook, you should now feel comfortable:
-- MAGIC * Processing CDC data with **`APPLY CHANGES INTO`**
-- MAGIC * Declaring live views
-- MAGIC * Joining live tables
-- MAGIC * Describing how DLT library notebooks work together in a pipeline
-- MAGIC * Scheduling multiple notebooks in a DLT pipeline
-- MAGIC
-- MAGIC In the next notebook, explore the output of our pipeline. Then we'll take a look at how to iteratively develop and troubleshoot DLT code.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>