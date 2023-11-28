# Databricks notebook source
# MAGIC %run ./Classroom-Setup-04-Common

# COMMAND ----------

lesson_config = LessonConfig(name = lesson_name,
                             create_schema = True,
                             create_catalog = False,
                             requires_uc = False,
                             installing_datasets = True,
                             enable_streaming_support = False,
                             enable_ml_support = False)

# Continues where 5.x picks up, don't remove assets
DA = DBAcademyHelper(course_config=course_config,
                     lesson_config=lesson_config)
# DA.reset_lesson()
DA.init()

# COMMAND ----------

pipeline_language = None
# The location that the DLT databases should be written to
DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"
DA.dlt_data_factory = DataFactory()

DA.dlt_data_factory.load()

DA.conclude_setup()

