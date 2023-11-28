# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

lesson_name = "jobs_demo"

# COMMAND ----------

class JobConfig():
    def __init__(self, job_name, notebook):
        self.job_name = job_name
        self.notebook = notebook
    
    def __repr__(self):
        content =  f"Name:      {self.job_name}"
        content += f"Notebooks: {self.notebook}"
        return content


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_job_config(self):
    
    unique_name = DA.unique_name(sep="-")
    job_name = f"{unique_name}: Example Job"
    
    parts = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[:-1]
    notebook = "/".join(parts) + "/DE 5.1.2 - Reset"

    return JobConfig(job_name, notebook)


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
def print_job_config_v1(self):
    "Provided by DBAcademy, this function renders the configuration of the job as HTML"
    job_config = self.get_job_config()
    
    displayHTML(f"""<table style="width:100%">
    <tr>
        <td style="white-space:nowrap; width:1em">Job Name:</td>
        <td><input type="text" value="{job_config.job_name}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Reset Notebook Path:</td>
        <td><input type="text" value="{job_config.notebook}" style="width:100%"></td></tr>

    </table>""")    
    

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_job_v1(self):
    "Provided by DBAcademy, this function creates the prescribed job"
    import re
    
    job_config = self.get_job_config()

    self.client.jobs.delete_by_name(job_config.job_name, success_only=False)
    cluster_id = dbgems.get_tags().get("clusterId")

    build_name = re.sub(r"[^a-zA-Z\d]", "-", self.course_config.course_name)
    while "--" in build_name: build_name = build_name.replace("--", "-")
    
    params = {
        "name": job_config.job_name,
        "tags": {
            "dbacademy.course": build_name,
            "dbacademy.source": build_name
        },
        "email_notifications": {},
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "tasks": [
            {
                "task_key": "Reset",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": job_config.notebook,
                    "base_parameters": []
                },
                "existing_cluster_id": cluster_id
            },
        ],
    }
    params = self.update_cluster_params(params, [0])
    
    create_response = self.client.jobs().create(params)
    job_id = create_response.get("job_id")
    
    print(f"Created job #{job_id}")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_job_v1_config(self):
    "Provided by DBAcademy, this function validates the configuration of the job"
    import json
    
    job_config = self.get_job_config()

    job = self.client.jobs.get_by_name(job_config.job_name)
    assert job is not None, f"The job named \"{job_config.job_name}\" doesn't exist. Double check the spelling."

    # print(json.dumps(job, indent=4))
    
    settings = job.get("settings")
    
    if settings.get("format") == "SINGLE_TASK":
        notebook_path = settings.get("notebook_task", {}).get("notebook_path")
        actual_cluster_id = settings.get("existing_cluster_id", None)
        #task_key = settings.get("task_key", None)
    else:
        tasks = settings.get("tasks", [])
        assert len(tasks) == 1, f"Expected one task, found {len(tasks)}."

        notebook_path = tasks[0].get("notebook_task", {}).get("notebook_path")
        actual_cluster_id = tasks[0].get("existing_cluster_id", None)
        
        task_key = tasks[0].get("task_key", None)
        assert task_key == "Rest", f"Expected the first task to have the name \"Reset\", found \"{task_key}\""
        
        
    assert notebook_path == job_config.notebook, f"Invalid Notebook Path. Found \"{notebook_path}\", expected \"{job_config.reset_notebook}\" "
    
    if not self.is_smoke_test():
        # Don't check the actual_cluster_id when running as a smoke test
        
        assert actual_cluster_id is not None, f"The first task is not configured to use the current All-Purpose cluster"

        expected_cluster_id = dbgems.get_tags().get("clusterId")
        if expected_cluster_id != actual_cluster_id:
            actual_cluster = self.client.clusters.get(actual_cluster_id).get("cluster_name")
            expected_cluster = self.client.clusters.get(expected_cluster_id).get("cluster_name")
            assert actual_cluster_id == expected_cluster_id, f"The first task is not configured to use the current All-Purpose cluster, expected \"{expected_cluster}\", found \"{actual_cluster}\""
        
    print("All tests passed!")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_job_v2(self):
    "Provided by DBAcademy, this function creates the prescribed job"
    import re
    
    job_config = self.get_job_config()
    pipeline_config = self.get_pipeline_config()

    self.client.jobs.delete_by_name(job_config.job_name, success_only=False)
    cluster_id = dbgems.get_tags().get("clusterId")
    
    pipeline = self.client.pipelines().get_by_name(pipeline_config.pipeline_name)
    pipeline_id = pipeline.get("pipeline_id")
    
    build_name = re.sub(r"[^a-zA-Z\d]", "-", self.course_config.course_name)
    while "--" in build_name: build_name = build_name.replace("--", "-")

    params = {
        "name": job_config.job_name,
        "tags": {
            "dbacademy.course": build_name,
            "dbacademy.source": build_name
        },
        "email_notifications": {},
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "tasks": [
            {
                "task_key": "Reset",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": job_config.notebook,
                    "base_parameters": []
                },
                "existing_cluster_id": cluster_id
            },
            {
                "task_key": "DLT",
                "depends_on": [ { "task_key": "Reset" } ],
                "pipeline_task": {
                    "pipeline_id": pipeline_id
                },
            },
        ],
    }
    params = self.update_cluster_params(params, [0])
    
    create_response = self.client.jobs().create(params)
    job_id = create_response.get("job_id")
    
    print(f"Created job #{job_id}")

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_job_v2_config(self):
    "Provided by DBAcademy, this function validates the configuration of the job"
    import json
    
    pipeline_config = self.get_pipeline_config()
    job_config = self.get_job_config()

    job = self.client.jobs.get_by_name(job_config.job_name)
    assert job is not None, f"The job named \"{job_config.job_name}\" doesn't exist. Double check the spelling."
    
    settings = job.get("settings")
    assert settings.get("format") == "MULTI_TASK", f"Expected two tasks, found 1."

    tasks = settings.get("tasks", [])
    assert len(tasks) == 2, f"Expected two tasks, found {len(tasks)}."
    
    
    # Reset Task
    task_name = tasks[0].get("task_key", None)
    assert task_name == "Reset", f"Expected the first task to have the name \"Reset\", found \"{task_name}\""
    
    notebook_path = tasks[0].get("notebook_task", {}).get("notebook_path")
    assert notebook_path == job_config.notebook, f"Invalid Notebook Path for the first task. Found \"{notebook_path}\", expected \"{job_config.notebook}\" "

    if not self.is_smoke_test():
        # Don't check the actual_cluster_id when running as a smoke test
        
        actual_cluster_id = tasks[0].get("existing_cluster_id", None)
        assert actual_cluster_id is not None, f"The first task is not configured to use the current All-Purpose cluster"

        expected_cluster_id = dbgems.get_tags().get("clusterId")
        if expected_cluster_id != actual_cluster_id:
            actual_cluster = self.client.clusters.get(actual_cluster_id).get("cluster_name")
            expected_cluster = self.client.clusters.get(expected_cluster_id).get("cluster_name")
            assert actual_cluster_id == expected_cluster_id, f"The first task is not configured to use the current All-Purpose cluster, expected \"{expected_cluster}\", found \"{actual_cluster}\""

    
    
    # Reset DLT
    task_name = tasks[1].get("task_key", None)
    assert task_name == "DLT", f"Expected the second task to have the name \"DLT\", found \"{task_name}\""

    actual_pipeline_id = tasks[1].get("pipeline_task", {}).get("pipeline_id", None)
    assert actual_pipeline_id is not None, f"The second task is not configured to use a Delta Live Tables pipeline"
    
    expected_pipeline = self.client.pipelines().get_by_name(pipeline_config.pipeline_name)
    actual_pipeline = self.client.pipelines().get_by_id(actual_pipeline_id)
    actual_name = actual_pipeline.get("spec").get("name", "Oops")
    assert actual_pipeline_id == expected_pipeline.get("pipeline_id"), f"The second task is not configured to use the correct pipeline, expected \"{pipeline_name}\", found \"{actual_name}\""
    
    depends_on = tasks[1].get("depends_on", [])
    assert len(depends_on) > 0, f"The \"DLT\" task does not depend on the \"Reset\" task"
    assert len(depends_on) == 1, f"The \"DLT\" task depends on more than just the \"Reset\" task"
    depends_task_key = depends_on[0].get("task_key")
    assert depends_task_key == "Reset", f"The \"DLT\" task doesn't depend on the \"Reset\" task, found {depends_task_key}"
    
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

# The DataFactory is just a pattern to demonstrate a fake stream is more of a function
# streaming workloads than it is of a pipeline - this pipeline happens to stream data.
class DataFactory:
    def __init__(self):
        
        # Bind the stream-source to DA because we will use it again later.
        DA.paths.stream_source = f"{DA.paths.working_dir}/stream-source"
        
        self.source_dir = f"{DA.paths.datasets}/retail-pipeline"
        self.target_dir = DA.paths.stream_source
        
        # All three datasets *should* have the same count, but just in case,
        # We are going to take the smaller count of the three datasets
        orders_count = len(dbutils.fs.ls(f"{self.source_dir}/orders/stream_json"))
        status_count = len(dbutils.fs.ls(f"{self.source_dir}/status/stream_json"))
        customer_count = len(dbutils.fs.ls(f"{self.source_dir}/customers/stream_json"))
        self.max_batch = min(min(orders_count, status_count), customer_count)
        
        self.current_batch = 0
        
    def load(self, continuous=False, delay_seconds=5):
        import time
        self.start = int(time.time())
        
        if self.current_batch >= self.max_batch:
            print("Data source exhausted\n")
            return False
        elif continuous:
            while self.load():
                time.sleep(delay_seconds)
            return False
        else:
            print(f"Loading batch {self.current_batch+1} of {self.max_batch}", end="...")
            self.copy_file("customers")
            self.copy_file("orders")
            self.copy_file("status")
            self.current_batch += 1
            print(f"{int(time.time())-self.start} seconds")
            return True
            
    def copy_file(self, dataset_name):
        source_file = f"{self.source_dir}/{dataset_name}/stream_json/{self.current_batch:02}.json/"
        target_file = f"{self.target_dir}/{dataset_name}/{self.current_batch:02}.json"
        dbutils.fs.cp(source_file, target_file)

# COMMAND ----------

class PipelineConfig():
    def __init__(self, pipeline_name, notebook):
        self.pipeline_name = pipeline_name  # The name of the pipeline
        self.notebook = notebook            # This list of notebooks for this pipeline
    
    def __repr__(self):
        content =  f"Name:     {self.pipeline_name}\n"
        content += f"Notebook: {self.notebook}"
        return content


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_pipeline_config(self):
    path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    notebook = "/".join(path.split("/")[:-1]) + "/DE 5.1.3 - DLT Job"
    
    unique_name = DA.unique_name(sep="-")
    pipeline_name = f"{unique_name}: Pipeline Demo w/Job"
    
    return PipelineConfig(pipeline_name, notebook)


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_pipeline(self):
    """
    Creates the prescribed pipeline.
    """
    
    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    config = self.get_pipeline_config()
    print(f"Creating the pipeline \"{config.pipeline_name}\"")

    # Delete the existing pipeline if it exists
    client.pipelines().delete_by_name(config.pipeline_name)

    
    # Create the new pipeline
    pipeline = client.pipelines().create(
        name = config.pipeline_name, 
        development=True,
        storage = self.paths.storage_location, 
        target = self.schema_name, 
        notebooks = [config.notebook], 
        configuration={
            "source": DA.paths.datasets,
        })
    
    self.pipeline_id = pipeline.get("pipeline_id")

    policy = self.client.cluster_policies.get_by_name("DBAcademy DLT-Only Policy")
    if policy is not None:
        self.client.pipelines.create_or_update(name = config.pipeline_name,
                                               storage = self.paths.storage_location,
                                               target = self.schema_name,
                                               notebooks = [config.notebook],
                                               configuration = {
                                                    "source": DA.paths.datasets,
                                                    "spark.master": "local[*]",
                                                },
                                                clusters=[{ 
                                                    "num_workers": 0,
                                                    "label": "default", 
                                                    "policy_id": policy.get("policy_id")
                                                }])
    
    displayHTML(f"""<table style="width:100%">
    <tr>
        <td style="white-space:nowrap; width:1em">Pipeline Name:</td>
        <td><input type="text" value="{config.pipeline_name}" style="width:100%"></td></tr>

    </table>""")    

