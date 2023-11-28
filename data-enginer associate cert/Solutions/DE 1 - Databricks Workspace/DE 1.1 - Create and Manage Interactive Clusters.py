# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Create and Manage Interactive Clusters
# MAGIC
# MAGIC A Databricks cluster is a set of computation resources and configurations on which you run data engineering, data science, and data analytics workloads, such as production ETL pipelines, streaming analytics, ad-hoc analytics, and machine learning. You run these workloads as a set of commands in a notebook or as an automated job. 
# MAGIC
# MAGIC Databricks makes a distinction between all-purpose clusters and job clusters. 
# MAGIC - You use all-purpose clusters to analyze data collaboratively using interactive notebooks.
# MAGIC - You use job clusters to run fast and robust automated jobs.
# MAGIC
# MAGIC This demo will cover creating and managing all-purpose Databricks clusters using the Databricks Data Science & Engineering Workspace. 
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Use the Clusters UI to configure and deploy a cluster
# MAGIC * Edit, terminate, restart, and delete clusters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Cluster
# MAGIC
# MAGIC Depending on the workspace in which you're currently working, you may or may not have cluster creation privileges. 
# MAGIC
# MAGIC Instructions in this section assume that you **do** have cluster creation privileges, and that you need to deploy a new cluster to execute the lessons in this course.
# MAGIC
# MAGIC **NOTE**: Check with your instructor or a platform admin to confirm whether or not you should create a new cluster or connect to a cluster that has already been deployed. Cluster policies may impact your options for cluster configuration. 
# MAGIC
# MAGIC Steps:
# MAGIC 1. Use the left sidebar to navigate to the **Compute** page by clicking on the ![compute](https://files.training.databricks.com/images/clusters-icon.png) icon
# MAGIC 1. Click the blue **Create Cluster** button
# MAGIC 1. For the **Cluster name**, use your name so that you can find it easily and the instructor can easily identify it if you have problems
# MAGIC 1. Set the **Cluster mode** to **Single Node** (this mode is required to run this course)
# MAGIC 1. Use the recommended **Databricks runtime version** for this course
# MAGIC 1. Leave boxes checked for the default settings under the **Autopilot Options**
# MAGIC 1. Click the blue **Create Cluster** button
# MAGIC
# MAGIC **NOTE:** Clusters can take several minutes to deploy. Once you have finished deploying a cluster, feel free to continue to explore the cluster creation UI.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### <img src="https://files.training.databricks.com/images/icon_warn_24.png"> Single-Node Cluster Required for This Course
# MAGIC **IMPORTANT:** This course requires you to run notebooks on a single-node cluster. 
# MAGIC
# MAGIC Follow the instructions above to create a cluster that has **Cluster mode** set to **`Single Node`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manage Clusters
# MAGIC
# MAGIC Once the cluster is created, go back to the **Compute** page to view the cluster.
# MAGIC
# MAGIC Select a cluster to review the current configuration. 
# MAGIC
# MAGIC Click the **Edit** button. Note that most settings can be modified (if you have sufficient permissions). Changing most settings will require running clusters to be restarted.
# MAGIC
# MAGIC **NOTE**: We'll be using our cluster in the following lesson. Restarting, terminating, or deleting your cluster may put you behind as you wait for new resources to be deployed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restart, Terminate, and Delete
# MAGIC
# MAGIC Note that while **Restart**, **Terminate**, and **Delete** have different effects, they all start with a cluster termination event. (Clusters will also terminate automatically due to inactivity assuming this setting is used.)
# MAGIC
# MAGIC When a cluster terminates, all cloud resources currently in use are deleted. This means:
# MAGIC * Associated VMs and operational memory will be purged
# MAGIC * Attached volume storage will be deleted
# MAGIC * Network connections between nodes will be removed
# MAGIC
# MAGIC In short, all resources previously associated with the compute environment will be completely removed. This means that **any results that need to be persisted should be saved to a permanent location**. Note that you will not lose your code, nor will you lose data files that you've saved out appropriately.
# MAGIC
# MAGIC The **Restart** button will allow us to manually restart our cluster. This can be useful if we need to completely clear out the cache on the cluster or wish to completely reset our compute environment.
# MAGIC
# MAGIC The **Terminate** button allows us to stop our cluster. We maintain our cluster configuration setting, and can use the **Restart** button to deploy a new set of cloud resources using the same configuration.
# MAGIC
# MAGIC The **Delete** button will stop our cluster and remove the cluster configuration.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>