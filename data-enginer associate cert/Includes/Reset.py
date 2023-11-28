# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

lesson_config.create_schema = False
lesson_config.installing_datasets = False

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_learning_environment()

# Created in each notebook from module, 06, we delete it here 
# to ensure that it is properly re-created when testing.
DA.client.scim.groups.delete_by_name("analyst")
DA.client.scim.groups.delete_by_name("analysts")
