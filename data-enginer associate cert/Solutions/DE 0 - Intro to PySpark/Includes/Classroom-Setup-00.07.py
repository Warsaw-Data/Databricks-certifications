# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

lesson_config = LessonConfig(name = None,
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


# COMMAND ----------

DA.paths.events_json = f"{DA.paths.datasets}/ecommerce/raw/events-500k-json"

# Create a user-specific copy of the users-csv
DA.paths.users_csv = f"{DA.paths.working_dir}/users-csv"
dbutils.fs.cp(f"{DA.paths.datasets}/ecommerce/raw/users-500k-csv", DA.paths.users_csv, True)

# COMMAND ----------

DA.conclude_setup()                      # Conclude setup by advertising environmental changes
