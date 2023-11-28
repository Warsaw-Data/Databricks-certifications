# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

def _create_silver_managed():
  import time
  start = int(time.time())
  print(f"Creating the table \"silver_managed\"", end="...")

  spark.sql("""
  CREATE OR REPLACE TABLE silver_managed (
    device_id  INT,
    mrn        STRING,
    name       STRING,
    time       TIMESTAMP,
    heartrate  DOUBLE)""")

  spark.sql("""
  INSERT INTO silver_managed VALUES
    (23,'40580129','Nicholas Spears','2020-02-01T00:01:58.000+0000',54.0122153343),
    (17,'52804177','Lynn Russell','2020-02-01T00:02:55.000+0000',92.5136468131),
    (37,'65300842','Samuel Hughes','2020-02-01T00:08:58.000+0000',52.1354807863),
    (23,'40580129','Nicholas Spears','2020-02-01T00:16:51.000+0000',54.6477014191),
    (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',95.033344842),
    (37,'65300842','Samuel Hughes','2020-02-01T00:23:58.000+0000',57.3391541312),
    (23,'40580129','Nicholas Spears','2020-02-01T00:31:58.000+0000',56.6165053697),
    (17,'52804177','Lynn Russell','2020-02-01T00:32:56.000+0000',94.8134313932),
    (37,'65300842','Samuel Hughes','2020-02-01T00:38:54.000+0000',56.2469995332),
    (23,'40580129','Nicholas Spears','2020-02-01T00:46:57.000+0000',54.8372685558)
  """)

  print(f"({int(time.time())-start} seconds)")

# COMMAND ----------

lesson_config = LessonConfig(name = None,
                             create_schema = False,
                             create_catalog = True,
                             requires_uc = True,
                             installing_datasets = True,
                             enable_streaming_support = False,
                             enable_ml_support = False)

DA = DBAcademyHelper(course_config=course_config,
                     lesson_config=lesson_config)
DA.reset_lesson()
DA.init()

_create_silver_managed()

DA.conclude_setup()

