# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

lesson_name = "jobs_lab"

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_pipeline_name(self):
    unique_name = DA.unique_name("-")
    return f"{unique_name}: Pipeline Lab w/Job"

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_pipeline(self):
    "Provided by DBAcademy, this function creates the prescribed pipline"
    
    path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    notebook = "/".join(path.split("/")[:-1]) + "/DE 5.2.3L - DLT Job"
    
    pipeline_name = self.get_pipeline_name()

    # We need to delete the existing pipline so that we can apply updates
    # because some attributes are not mutable after creation.
    self.client.pipelines().delete_by_name(pipeline_name)
    
    clusters = [{ 
        "label": "default", 
        "num_workers": 0 
    }]
    
    policy = self.client.cluster_policies.get_by_name("DBAcademy DLT-Only Policy")
    if policy is not None:
        clusters = [{ 
            "label": "default", 
            "num_workers": 0,
            "policy_id": policy.get("policy_id")
        }]
        
    response = self.client.pipelines().create(
        name = pipeline_name, 
        storage = DA.paths.storage_location, 
        target = DA.schema_name, 
        notebooks = [notebook],
        configuration = {
            "spark.master": "local[*]",
            "datasets_path": DA.paths.datasets,
            "source": DA.paths.stream_path,
        },
        clusters=clusters)
    
    pipeline_id = response.get("pipeline_id")

    displayHTML(f"""<table style="width:100%">
    <tr>
        <td style="white-space:nowrap; width:1em">Pipeline Name:</td>
        <td><input type="text" value="{pipeline_name}" style="width:100%"></td></tr>
    
    </table>""")


# COMMAND ----------

class JobConfig():
    def __init__(self, job_name, notebook_1, notebook_2):
        self.job_name = job_name
        self.notebook_1 = notebook_1
        self.notebook_2 = notebook_2
    
    def __repr__(self):
        content =  f"Name:      {self.job_name}"
        content += f"Notebooks: {self.notebook}"
        return content


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_job_config(self):
    
    unique_name = DA.unique_name("-")
    job_name = f"{unique_name}: Jobs Lab"
    
    notebook_1 = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    notebook_1 = "/".join(notebook_1.split("/")[:-1]) + "/DE 5.2.2L - Batch Job"

    notebook_2 = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    notebook_2 = "/".join(notebook_2.split("/")[:-1]) + "/DE 5.2.4L - Query Results Job"

    return JobConfig(job_name, notebook_1, notebook_2)


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def print_job_config(self):
    
    job_config = self.get_job_config()
    
    displayHTML(f"""<table style="width:100%">
    <tr>
        <td style="white-space:nowrap; width:1em">Job Name:</td>
        <td><input type="text" value="{job_config.job_name}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Batch Notebook Path:</td>
        <td><input type="text" value="{job_config.notebook_1}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Query Notebook Path:</td>
        <td><input type="text" value="{job_config.notebook_2}" style="width:100%"></td></tr>
        
    </table>""")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def update_cluster_params(self, params: dict, task_indexes: list):

    if not self.is_smoke_test():
        return params
    
    for task_index in task_indexes:
        # Need to modify the parameters to run run as a smoke-test.
        task = params.get("tasks")[task_index]
        del task["existing_cluster_id"]

        cluster_params =         {
            "num_workers": "0",
            "spark_version": self.client.clusters().get_current_spark_version(),
            "spark_conf": {
              "spark.master": "local[*]"
            },
        }

        instance_pool_id = self.client.clusters().get_current_instance_pool_id()
        if instance_pool_id is not None: cluster_params["instance_pool_id"] = self.client.clusters().get_current_instance_pool_id()
        else:                            cluster_params["node_type_id"] = self.client.clusters().get_current_node_type_id()

        task["new_cluster"] = cluster_params
        
    return params


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_job(self):
    "Provided by DBAcademy, this function creates the prescribed job"
    
    pipeline_name = self.get_pipeline_name()
    job_config = self.get_job_config()

    self.client.jobs.delete_by_name(job_config.job_name, success_only=False)
    cluster_id = dbgems.get_tags().get("clusterId")
    
    pipeline = self.client.pipelines().get_by_name(pipeline_name)
    pipeline_id = pipeline.get("pipeline_id")
    
    params = {
        "name": job_config.job_name,
        "tags": {
            "dbacademy.course": self.course_config.course_name,
            "dbacademy.source": self.course_config.course_name
        },
        "email_notifications": {},
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "tasks": [
            {
                "task_key": "Batch-Job",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": job_config.notebook_1,
                    "base_parameters": []
                },
                "existing_cluster_id": cluster_id
            },
            {
                "task_key": "DLT",
                "depends_on": [ { "task_key": "Batch-Job" } ],
                "pipeline_task": {
                    "pipeline_id": pipeline_id
                },
            },
            {
                "task_key": "Query-Results",
                "depends_on": [ { "task_key": "DLT" } ],
                "libraries": [],
                "notebook_task": {
                    "notebook_path": job_config.notebook_2,
                    "base_parameters": []
                },
                "existing_cluster_id": cluster_id
            },
        ],
    }
    params = self.update_cluster_params(params, [0,2])
    
    create_response = self.client.jobs().create(params)
    job_id = create_response.get("job_id")
    
    print(f"Created job \"{job_config.job_name}\" (#{job_id})")

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_job_config(self):
    "Provided by DBAcademy, this function validates the configuration of the job"
    import json
    
    pipeline_name = self.get_pipeline_name()
    job_config = self.get_job_config()

    job = self.client.jobs.get_by_name(job_config.job_name)
    assert job is not None, f"The job named \"{job_name}\" doesn't exist. Double check the spelling."
    
    settings = job.get("settings")
    assert settings.get("format") == "MULTI_TASK", f"Expected three tasks, found 1."

    tasks = settings.get("tasks", [])
    assert len(tasks) == 3, f"Expected three tasks, found {len(tasks)}."

    
    
    # Batch-Job Task
    batch_task = tasks[0]
    task_name = batch_task.get("task_key", None)
    assert task_name == "Batch-Job", f"Expected the first task to have the name \"Batch-Job\", found \"{task_name}\""
    
    notebook_path = batch_task.get("notebook_task", {}).get("notebook_path")
    assert notebook_path == job_config.notebook_1, f"Invalid Notebook Path for the first task. Found \"{notebook_path}\", expected \"{job_config.notebook_1}\" "

    if not self.is_smoke_test():
        # Don't check the actual_cluster_id when running as a smoke test
        
        actual_cluster_id = batch_task.get("existing_cluster_id", None)
        assert actual_cluster_id is not None, f"The first task is not configured to use the current All-Purpose cluster"

        expected_cluster_id = dbgems.get_tags().get("clusterId")
        if expected_cluster_id != actual_cluster_id:
            actual_cluster = self.client.clusters.get(actual_cluster_id).get("cluster_name")
            expected_cluster = self.client.clusters.get(expected_cluster_id).get("cluster_name")
            assert actual_cluster_id == expected_cluster_id, f"The first task is not configured to use the current All-Purpose cluster, expected \"{expected_cluster}\", found \"{actual_cluster}\""

    
    
    # DLT
    dlt_task = tasks[1]
    task_name = dlt_task.get("task_key", None)
    assert task_name == "DLT", f"Expected the second task to have the name \"DLT\", found \"{task_name}\""

    actual_pipeline_id = dlt_task.get("pipeline_task", {}).get("pipeline_id", None)
    assert actual_pipeline_id is not None, f"The second task is not configured to use a Delta Live Tables pipeline"
    
    expected_pipeline = self.client.pipelines().get_by_name(pipeline_name)
    actual_pipeline = self.client.pipelines().get_by_id(actual_pipeline_id)
    actual_name = actual_pipeline.get("spec").get("name", "Oops")
    assert actual_pipeline_id == expected_pipeline.get("pipeline_id"), f"The second task is not configured to use the correct pipeline, expected \"{pipeline_name}\", found \"{actual_name}\""
    
    depends_on = dlt_task.get("depends_on", [])
    assert len(depends_on) > 0, f"The \"DLT\" task does not depend on the \"Batch-Job\" task"
    assert len(depends_on) == 1, f"The \"DLT\" task depends on more than just the \"Batch-Job\" task"
    depends_task_key = depends_on[0].get("task_key")
    assert depends_task_key == "Batch-Job", f"The \"DLT\" task doesn't depend on the \"Batch-Job\" task, found \"{depends_task_key}\"."
    
    
    
    # Query Task
    query_task = tasks[2] 
    task_name = query_task.get("task_key", None)
    assert task_name == "Query-Results", f"Expected the third task to have the name \"Query-Results\", found \"{task_name}\""
    
    notebook_path = query_task.get("notebook_task", {}).get("notebook_path")
    assert notebook_path == job_config.notebook_2, f"Invalid Notebook Path for the thrid task. Found \"{notebook_path}\", expected \"{job_config.notebook_2}\" "
    
    depends_on = query_task.get("depends_on", [])
    assert len(depends_on) > 0, f"The \"Query-Results\" task does not depend on the \"DLT\" task"
    assert len(depends_on) == 1, f"The \"Query-Results\" task depends on more than just the \"DLT\" task"
    depends_task_key = depends_on[0].get("task_key")
    assert depends_task_key == "DLT", f"The \"Query-Results\" task doesn't depend on the \"DLT\" task, found \"{depends_task_key}\"."

    if not self.is_smoke_test():
        # Don't check the actual_cluster_id when running as a smoke test
        
        actual_cluster_id = query_task.get("existing_cluster_id", None)
        assert actual_cluster_id is not None, f"The second task is not configured to use the current All-Purpose cluster"

        expected_cluster_id = dbgems.get_tags().get("clusterId")
        if expected_cluster_id != actual_cluster_id:
            actual_cluster = self.client.clusters.get(actual_cluster_id).get("cluster_name")
            expected_cluster = self.client.clusters.get(expected_cluster_id).get("cluster_name")
            assert actual_cluster_id == expected_cluster_id, f"The second task is not configured to use the current All-Purpose cluster, expected \"{expected_cluster}\", found \"{actual_cluster}\""

    print("All tests passed!")

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def start_job(self):
    job_config = self.get_job_config()
    job_id = self.client.jobs.get_by_name(job_config.job_name).get("job_id")
    run_id = self.client.jobs.run_now(job_id).get("run_id")
    print(f"Started job #{job_id}, run #{run_id}")

    self.client.runs.wait_for(run_id)

# COMMAND ----------

class DataFactory:
    def __init__(self, stream_path):
        self.stream_path = stream_path
        self.source = f"{DA.paths.datasets}/healthcare/tracker/streaming"
        try:
            self.curr_mo = 1 + int(max([x[1].split(".")[0] for x in dbutils.fs.ls(self.stream_path)]))
        except:
            self.curr_mo = 1
    
    def load(self, continuous=False):
        if self.curr_mo > 12:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.curr_mo <= 12:
                curr_file = f"{self.curr_mo:02}.json"
                target_dir = f"{self.stream_path}/{curr_file}"
                print(f"Loading the file {curr_file} to the {target_dir}")
                dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
                self.curr_mo += 1
        else:
            curr_file = f"{str(self.curr_mo).zfill(2)}.json"
            target_dir = f"{self.stream_path}/{curr_file}"
            print(f"Loading the file {curr_file} to the {target_dir}")

            dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
            self.curr_mo += 1
