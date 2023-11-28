# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

# MAGIC %run ./Classroom-Setup-00-common

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

# Create a user-specific copy of the products-csv
DA.paths.products_csv = f"{DA.paths.working_dir}/products-csv"
dbutils.fs.cp(f"{DA.paths.datasets}/ecommerce/raw/products-csv", DA.paths.products_csv, True)

DA.conclude_setup()                      # Conclude setup by advertising environmental changes
