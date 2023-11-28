-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create compute sources for Unity Catalog access
-- MAGIC
-- MAGIC In this demo you will learn how to:
-- MAGIC * Configure a cluster for Unity Catalog access
-- MAGIC * Configure a SQL warehouse for Unity Catalog access
-- MAGIC * Use the Data Explorer to browse the Unity Catalog three-level namespace

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overview
-- MAGIC
-- MAGIC Prior to Unity Catalog, a full data governance solution required careful configuration of workspace settings, access control lists, and cluster settings and policies. It was a co-operative system that, if not properly set up, could allow users to bypass access control altogether.
-- MAGIC
-- MAGIC While Unity Catalog introduces some new settings, the system does not require any specific configuration to be secure. Without proper settings, clusters will not be able to access any secured data at all, making Unity Catalog secure by default. This, coupled with its improved fine-grained control and auditing, makes Unity Catalog a significantly evolved data governance solution.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC
-- MAGIC If you would like to follow along with this demo, you will need the **Allow unrestricted cluster creation** entitlement. Consult with your workspace administrator if you do not have the ability to create clusters or SQL warehouses.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Science and Engineering Workspace
-- MAGIC
-- MAGIC Apart from SQL warehouses, clusters are the gateway to your data, as they are the workhorses responsible for running the code in your notebooks. In this section we see how to configure an all-purpose cluster to access Unity Catalog. The same configuration principles can be applied when configuring job clusters for running automated workloads.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a cluster
-- MAGIC
-- MAGIC Let’s create an all-purpose cluster capable of accessing Unity Catalog. For existing Databricks users, this procedure will be familiar, but we introduce and explore some new settings.
-- MAGIC
-- MAGIC 1. In the Data Science and Engineering Workspace, click on **Compute** in the left sidebar.
-- MAGIC 1. Now let’s click **Create Cluster**.
-- MAGIC 1. Let's specify a name for our cluster. This must be unique in the workspace.
-- MAGIC 1. Now let's take a look at the **Cluster mode** setting. Note that with Unity Catalog, the *High Concurrency* option, which existing Databricks users may have used in the past, is deprecated. One of the primary use cases behind this option related to table access control prior to Unity Catalog, but a new parameter called **Security mode** has been introduced to handle this. This leaves us with *Standard* and *Single node*, which really you only need to use if you have a specific need for a single node cluster. So let’s leave cluster mode set to *Standard*.
-- MAGIC 1. Now let’s choose a **Databricks runtime version** that is 11.1 or higher.
-- MAGIC 1. For the purpose of reducing compute cost for this demo, I will disable autoscaling and reduce the number of workers, although it’s not necessary to do this.
-- MAGIC 1. Now, we must configure the **Security mode**, which is accessible in the **Advanced options**. There are five options available, but three of these are not compatible with Unity Catalog. The two options we need to consider are as follows:
-- MAGIC     * *User isolation* clusters can be shared by multiple users, but overall functionality is reduced to promote a secure, isolated environment. Only Python and SQL are allowed, and some advanced cluster features such as library installation, init scripts and the DBFS Fuse mounts are also unavailable.
-- MAGIC     * *Single user* clusters support all languages and features, but the cluster can only be used exclusively by a single user (by default, the owner of the cluster). This is the recommended choice for job clusters and interactive clusters, if you need any of the features not offered in *User isolation* mode.
-- MAGIC    
-- MAGIC    Let’s choose *Single user*. Note that when we choose this mode, a new field called **Single user access** appears.
-- MAGIC 1. The **Single user acess** setting allows us to designate who can attach to this cluster. This setting is independent and unrelated to cluster access controls, which existing Databricks users might be familiar with. This setting is enforced on the cluster, and only the user designated here will be able to attach; all others will be rejected. By default this is set to ourselves, which we will leave as is. But consider changing this setting when one of the following apply:
-- MAGIC     * You are creating an all-purpose cluster for someone else
-- MAGIC     * You are configuring a job cluster, in which case this should be set to the service principal that will be running the job
-- MAGIC 1. Let’s click **Create Cluster**. This operation takes a few moments, so let's wait for our cluster to start.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Browsing data
-- MAGIC
-- MAGIC Let's take a few moments to get aquainted with the updated **Data** page in the Data Science and Engineering Workspace.
-- MAGIC
-- MAGIC 1. Click on **Data** in the left sidebar. Notice that we are presented now with three columns, where previously there were two:
-- MAGIC     * **Catalogs** allows us to select a catalog, the top-level container in Unity Catalog's data object hierarchy, and the first part of the three-level namespace
-- MAGIC     * **Databases**, also referred to as schemas, allows us to select a schema within the selected catalog. This is the second part of the three-level namespace (or the first part of the traditional two-level namespace most will be familiar with)
-- MAGIC     * **Tables** allows us to select a table to explore. From here we can view a table's metadata, history, and get a sample data snapshot
-- MAGIC 1. Let's explore the hierarchy. The catalogs, schemas and tables you can select depend on how populated your metastore is, and permissions.
-- MAGIC 1. The catalog labeled *main* is created by default as part of the metastore creation process (similar to a *default* schema)
-- MAGIC 1. The catalog labeled *hive_metastore* corresponds to the Hive metastore local to the workspace.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Explore data using a notebook
-- MAGIC
-- MAGIC Let's do a quick check using a notebook.
-- MAGIC
-- MAGIC 1. Let's create a new SQL notebook in the workspace to run a short test. Let's attach the notebook to the cluster we created.
-- MAGIC 1. Create a new cell with the query `SHOW GRANTS ON SCHEMA main.default` and run it.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Databricks SQL
-- MAGIC
-- MAGIC SQL warehouses are the other primary means for accessing your data, as they are the compute resources responsible for running your Dataricks SQL queries. If you want to enable connectivity to external business intelligence (BI) tools, you will also need to channel those through a SQL warehouse.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a SQL warehouse
-- MAGIC
-- MAGIC Because SQL warehouses are purpose-built cluster configurations, Unity Catalog configuration is even easier, as we will see here.
-- MAGIC
-- MAGIC 1. Switch to the **SQL** persona.
-- MAGIC 1. Click **SQL Warehouses** in the left sidebar.
-- MAGIC 1. Click **Create SQL Warehouse**.
-- MAGIC 1. Let's specify a name for our warehouse. This must be unique in the workspace.
-- MAGIC 1. For the purpose of reducing cost for this lab environment, I will choose a *2X-Small* cluster size and also set the scaling max to 1.
-- MAGIC 1. In the **Advanced options**, let's ensure that **Unity Catalog** is enabled.
-- MAGIC 1. With configuration complete, let’s click **Create**.
-- MAGIC 1. Let's make the cluster accessible by everyone in the workspace. In the **Manage permissions dialog**, let's add a new permission:
-- MAGIC     1. Click the search field and select *All Users*.
-- MAGIC     1. Leave the permission set to *Can use* and click **Add**.
-- MAGIC     
-- MAGIC This operation takes a few moments, so let's wait for our warehouse to start.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Browsing data
-- MAGIC
-- MAGIC Let's take a few moments to get aquainted with the updated **Data Explorer** page.
-- MAGIC
-- MAGIC 1. Click on **Data** in the left sidebar. The user interface layout is different than the Data Science and Engineering Workspace. Notice how we are presented with a hierarchical view of the metastore, yet still presenting the same elements.
-- MAGIC 1. Let's explore the hierarchy. As before, the catalogs, schemas and tables you can select depend on how populated your metastore is, and permissions.
-- MAGIC 1. From here we can also create objects or manage permissions, however we will reserve these tasks for a later lab.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Explore data using queries
-- MAGIC
-- MAGIC Let's repeat the queries we did earlier in the Data Science and Engineering workspace, this time creating a new DBSQL query and running it using the SQL warehouse we just created. As a recap, the query was `SHOW GRANTS ON SCHEMA main.default`.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>