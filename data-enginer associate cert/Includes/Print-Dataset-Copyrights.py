# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

lesson_config = LessonConfig(name = None,
                             create_schema = False,
                             create_catalog = False,
                             requires_uc = False,
                             installing_datasets = True,
                             enable_streaming_support = False,
                             enable_ml_support = False)

DA = DBAcademyHelper(course_config=course_config,
                     lesson_config=lesson_config)
DA.init()

# COMMAND ----------

DA.print_copyrights()
