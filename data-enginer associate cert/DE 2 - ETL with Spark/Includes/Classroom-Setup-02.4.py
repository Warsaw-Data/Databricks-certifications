# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

# lesson: Writing delta 
def _create_eltwss_users_update():
    import time
    import pyspark.sql.functions as F
    start = int(time.time())
    print(f"\nCreating the table \"users_dirty\"", end="...")

    df = spark.createDataFrame(data=[(None, None, None, None), (None, None, None, None), (None, None, None, None)], 
                               schema="user_id: string, user_first_touch_timestamp: long, email:string, updated:timestamp")
    (spark.read
          .parquet(f"{DA.paths.datasets}/ecommerce/raw/users-30m")
          .withColumn("updated", F.current_timestamp())
          .union(df)
          .write
          .mode("overwrite")
          .saveAsTable("users_dirty"))
    
    total = spark.read.table("users_dirty").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")

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

_create_eltwss_users_update()
    
DA.conclude_setup()
