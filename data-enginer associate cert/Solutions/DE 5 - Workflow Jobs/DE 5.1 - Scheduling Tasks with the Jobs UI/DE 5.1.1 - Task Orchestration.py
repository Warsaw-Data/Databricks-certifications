# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Orchestrating Jobs with Databricks Workflows
# MAGIC
# MAGIC New updates to the Databricks Jobs UI have added the ability to schedule multiple tasks as part of a job, allowing Databricks Jobs to fully handle orchestration for most production workloads.
# MAGIC
# MAGIC Here, we'll start by reviewing the steps for scheduling a notebook task as a triggered standalone job, and then add a dependent task using a DLT pipeline. 
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Schedule a notebook task in a Databricks Workflow Job
# MAGIC * Describe job scheduling options and differences between cluster types
# MAGIC * Review Job Runs to track progress and see results
# MAGIC * Schedule a DLT pipeline task in a Databricks Workflow Job
# MAGIC * Configure linear dependencies between tasks using the Databricks Workflows UI

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-05.1.1 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Generate Job Configuration
# MAGIC
# MAGIC Configuring this job will require parameters unique to a given user.
# MAGIC
# MAGIC Run the cell below to print out values you'll use to configure your pipeline in subsequent steps.

# COMMAND ----------

DA.print_job_config_v1()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configure Job with a Single Notebook Task
# MAGIC
# MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by creating a job with a single task.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Click the **Workflows** button on the sidebar, click the **Jobs** tab, and click the **Create Job** button.
# MAGIC 2. Configure the job and task as specified below. You'll need the values provided in the cell output above for this step.
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | Enter **Reset** |
# MAGIC | Type | Choose **Notebook** |
# MAGIC | Source | Choose **Workspace** |
# MAGIC | Path | Use the navigator to specify the **Reset Notebook Path** provided above |
# MAGIC | Cluster | From the dropdown menu, under **Existing All Purpose Clusters**, select your cluster |
# MAGIC | Job name | In the top-left of the screen, enter the **Job Name** provided above to add a name for the job (not the task) |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. Click the **Create** button.
# MAGIC 4. Click the blue **Run now** button in the top right to start the job.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all-purpose cluster, you will get a warning about how this will be billed as all-purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job_v1()

# COMMAND ----------

DA.validate_job_v1_config()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Explore Scheduling Options
# MAGIC Steps:
# MAGIC 1. On the right hand side of the Jobs UI, locate the **Job Details** section.
# MAGIC 1. Under the **Trigger** section, select the **Add trigger** button to explore scheduling options.
# MAGIC 1. Changing the **Trigger type** from **None (Manual)** to **Scheduled** will bring up a cron scheduling UI.
# MAGIC    - This UI provides extensive options for setting up chronological scheduling of your Jobs. Settings configured with the UI can also be output in cron syntax, which can be edited if custom configuration not available with the UI is needed.
# MAGIC 1. At this time, we'll leave our job set to **Manual** scheduling; select **Cancel** to return to Job details.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Review Run
# MAGIC
# MAGIC To review the job run:
# MAGIC 1. On the Jobs details page, select the **Runs** tab in the top-left of the screen (you should currently be on the **Tasks** tab)
# MAGIC 1. Find your job.
# MAGIC     - If **the job is still running**, it will be under the **Active runs** section. 
# MAGIC     - If **the job finished running**, it will be under the **Completed runs** section
# MAGIC 1. Open the output details by clicking on the timestamp field under the **Start time** column
# MAGIC     - If **the job is still running**, you will see the active state of the notebook with a **Status** of **`Pending`** or **`Running`** in the right side panel. 
# MAGIC     - If **the job has completed**, you will see the full execution of the notebook with a **Status** of **`Succeeded`** or **`Failed`** in the right side panel
# MAGIC   
# MAGIC The notebook employs the magic command **`%run`** to call an additional notebook using a relative path. Note that while not covered in this course, <a href="https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo" target="_blank">new functionality added to Databricks Repos allows loading Python modules using relative paths</a>.
# MAGIC
# MAGIC The actual outcome of the scheduled notebook is to reset the environment for our new job and pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Pipeline
# MAGIC
# MAGIC In this step, we'll add a DLT pipeline to execute after the success of the task we configured at the start of this lesson.
# MAGIC
# MAGIC To focus on jobs and not pipelines, we are going to use the following utility command to create a simple pipeline for us.

# COMMAND ----------

DA.create_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configure a DLT Pipeline Task
# MAGIC
# MAGIC Next, we need to add the task to run this pipeline.
# MAGIC
# MAGIC Steps:
# MAGIC 1. On the Job details page, click the **Tasks** tab.
# MAGIC 1. Click the large blue circle with a **+** at the center bottom of the screen to add a new task
# MAGIC 1. Configure the task as specified below.
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | Enter **DLT** |
# MAGIC | Type | Choose **Delta Live Tables pipeline** |
# MAGIC | Pipeline | Choose the DLT pipeline configured above |
# MAGIC | Depends on | Choose **Reset**, which is the previous task we defined |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 4. Click the blue **Create task** button
# MAGIC     - You should now see a screen with 2 boxes and a downward arrow between them. 
# MAGIC     - Your **`Reset`** task will be at the top, leading into your **`DLT`** task. 
# MAGIC     - This visualization represents the dependencies between these tasks.
# MAGIC 5. Validate the configuration by running the command below.
# MAGIC     - If errors are reported, repeat the following until all errors have been removed.
# MAGIC       - Fix the error(s).
# MAGIC       - Click the **Create task** button.
# MAGIC       - Validate the configuration.

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job_v2()

# COMMAND ----------

DA.validate_job_v2_config()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run the job
# MAGIC Once the job has been properly configured, click the blue **Run now** button in the top right to start the job.
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all-purpose cluster, you will get a warning about how this will be billed as all-purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.
# MAGIC
# MAGIC **NOTE**: You may need to wait a few minutes as infrastructure for your job and pipeline is deployed.

# COMMAND ----------

# ANSWER

# This function is provided to start the pipeline and  
# block until it has completed, canceled or failed
DA.start_job()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Review Multi-Task Run Results
# MAGIC
# MAGIC To review run results:
# MAGIC 1. On the Job details page, select the **Runs** tab again and then the most recent run under **Active runs** or **Completed runs** depending on if the job has completed or not.
# MAGIC     - The visualizations for tasks will update in real time to reflect which tasks are actively running, and will change colors if task failures occur. 
# MAGIC 1. Clicking on a task box will render the scheduled notebook in the UI. 
# MAGIC     - You can think of this as just an additional layer of orchestration on top of the previous Databricks Jobs UI, if that helps;
# MAGIC     - Note that if you have workloads scheduling jobs with the CLI or REST API, <a href="https://docs.databricks.com/dev-tools/api/latest/jobs.html" target="_blank">the JSON structure used to configure and get results about jobs has seen similar updates to the UI</a>.
# MAGIC
# MAGIC **NOTE**: At this time, DLT pipelines scheduled as tasks do not directly render results in the Runs GUI; instead, you will be directed back to the DLT Pipeline GUI for the scheduled Pipeline.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>