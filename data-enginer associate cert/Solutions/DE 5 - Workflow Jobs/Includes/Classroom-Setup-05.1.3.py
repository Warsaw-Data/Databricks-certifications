# Databricks notebook source
# MAGIC %run ./Classroom-Setup-05.1-Common

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
# DA.reset_lesson()

DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"
DA.init()

DA.conclude_setup()

