# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Workspace Setup
# MAGIC This notebook should be run by instructors to prepare the workspace for a class.
# MAGIC
# MAGIC The key changes this notebook makes includes:
# MAGIC * Updating user-specific grants such that they can create databases/schemas against the current catalog when they are not workspace-admins.
# MAGIC * Configures three cluster policies:
# MAGIC     * **DBAcademy** - which should be used on clusters running standard notebooks.
# MAGIC     * **DBAcademy Jobs** - which should be used on workflows/jobs
# MAGIC     * **DBAcademy DLT** - which should be used on DLT piplines (automatically applied)
# MAGIC * Create or update the shared **DBAcademy Warehouse** for use in Databricks SQL exercises
# MAGIC * Create the Instance Pool **DBAcademy** for use by students and the "student" and "jobs" policies.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# Start a timer so we can benchmark execution duration.
setup_start = dbgems.clock_start()

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Class Config
# MAGIC The three variables defined by these widgets are used to configure our environment as a means of controlling class cost.

# COMMAND ----------

from dbacademy.dbhelper import WorkspaceHelper

# Setup the widgets to collect required parameters.
dbutils.widgets.dropdown("configure_for", WorkspaceHelper.CONFIGURE_FOR_ALL_USERS, 
                         [WorkspaceHelper.CONFIGURE_FOR_ALL_USERS], "Configure For (required)")

# lab_id is the name assigned to this event/class or alternatively its class number
dbutils.widgets.text(WorkspaceHelper.PARAM_LAB_ID, "", "Lab/Class ID (optional)")

# a general purpose description of the class
dbutils.widgets.text(WorkspaceHelper.PARAM_DESCRIPTION, "", "Description (optional)")

# The default spark version
dbutils.widgets.text(WorkspaceHelper.PARAM_SPARK_VERSION, "11.3.x-cpu-ml-scala2.12", "Spark Version (optional)")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Init Script & Install Datasets
# MAGIC The main affect of this call is to pre-install the datasets.
# MAGIC
# MAGIC It has the side effect of create our DA object which includes our REST client.

# COMMAND ----------

lesson_config.create_schema = False

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()
DA.conclude_setup()

# COMMAND ----------

from dbacademy.dbhelper import ClustersHelper

org_id = dbgems.get_org_id()
lab_id = WorkspaceHelper.get_lab_id() or "UNKNOWN"
spark_version = WorkspaceHelper.get_spark_version()
workspace_name = WorkspaceHelper.get_workspace_name()
workspace_description = WorkspaceHelper.get_workspace_description() or "UNKNOWN"

print(f"org_id:                {org_id}")
print(f"lab_id:                {lab_id}")
print(f"spark_version:         {spark_version}")
print(f"workspace_name:        {workspace_name}")
print(f"workspace_description: {workspace_description}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Class Instance Pools
# MAGIC The following cell configures the instance pool used for this class

# COMMAND ----------

instance_pool_id = DA.workspace.clusters.create_instance_pool(preloaded_spark_version=spark_version,
                                                              org_id=org_id, 
                                                              lab_id=lab_id, 
                                                              workspace_name=workspace_name, 
                                                              workspace_description=workspace_description)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create The Three Class-Specific Cluster Policies
# MAGIC The following cells create the various cluster policies used by the class

# COMMAND ----------

# org_id, lab_id, workspace_name and workspace_description are attached to the
# instance pool and as such, they are not attached to the all-purpose or jobs policies.

ClustersHelper.create_all_purpose_policy(client=DA.client, 
                                         instance_pool_id=instance_pool_id, 
                                         spark_version=spark_version,
                                         autotermination_minutes_max=180,
                                         autotermination_minutes_default=120)

ClustersHelper.create_jobs_policy(client=DA.client, 
                                  instance_pool_id=instance_pool_id, 
                                  spark_version=spark_version)

ClustersHelper.create_dlt_policy(client=DA.client, 
                                 org_id=org_id, 
                                 lab_id=lab_id, 
                                 workspace_name=workspace_name, 
                                 workspace_description=workspace_description)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Class-Shared Databricks SQL Warehouse/Endpoint
# MAGIC Creates a single wharehouse to be used by all students.
# MAGIC
# MAGIC The configuration is derived from the number of students specified above.

# COMMAND ----------

from dbacademy.dbhelper.warehouses_helper_class import WarehousesHelper

DA.workspace.warehouses.create_shared_sql_warehouse(name=WarehousesHelper.WAREHOUSES_DEFAULT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configure User Entitlements
# MAGIC
# MAGIC This task simply adds the "**databricks-sql-access**" entitlement to the "**users**" group ensuring that they can access the Databricks SQL view.

# COMMAND ----------

WorkspaceHelper.add_entitlement_workspace_access(client=DA.client)
WorkspaceHelper.add_entitlement_databricks_sql_access(client=DA.client)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Update Grants
# MAGIC This operation executes **`GRANT CREATE ON CATALOG TO users`** to ensure that students can create databases as required by this course when they are not admins.
# MAGIC
# MAGIC Note: The implementation requires this to execute in another job and as such can take about three minutes to complete.

# COMMAND ----------

from dbacademy.dbhelper.databases_helper_class import DatabasesHelper

# Ensures that all users can create databases on the current catalog 
# for cases wherein the user/student is not an admin.
job_id = DatabasesHelper.configure_permissions(DA.client, "Configure-Permissions", "10.4.x-scala2.12")

# COMMAND ----------

DA.client.jobs().delete_by_id(job_id)

# COMMAND ----------

# MAGIC %md
# MAGIC Executing the operation below ensures that students can create catalogs as required by Unity Catalog content in this course when they are not admins.

# COMMAND ----------

spark.sql("GRANT CREATE CATALOG ON METASTORE TO `account users`;")

# COMMAND ----------

print(f"Setup completed {dbgems.clock_stopped(setup_start)}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>