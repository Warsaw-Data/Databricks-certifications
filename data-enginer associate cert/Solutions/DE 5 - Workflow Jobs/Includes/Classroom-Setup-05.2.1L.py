# Databricks notebook source
# MAGIC %run ./Classroom-Setup-05.2-Common

# COMMAND ----------

lesson_config = LessonConfig(name = lesson_name,
                             create_schema = True,
                             create_catalog = False,
                             requires_uc = False,
                             installing_datasets = True,
                             enable_streaming_support = False,
                             enable_ml_support = False)

DA = DBAcademyHelper(course_config=course_config,
                     lesson_config=lesson_config)
DA.reset_lesson()
DA.init()

DA.paths.stream_path = f"{DA.paths.working_dir}/stream"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"

DA.data_factory = DataFactory(DA.paths.stream_path)

DA.conclude_setup()

