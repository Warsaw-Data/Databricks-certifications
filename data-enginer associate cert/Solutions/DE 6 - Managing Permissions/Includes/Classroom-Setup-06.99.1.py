# Databricks notebook source
# MAGIC %run ../../Includes/_common

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
DA.conclude_setup()

# COMMAND ----------

# Create user-specific schema name here (lesson config settings above creates user-specific catalog with DA.schema_name as "default")
DA.my_schema_name = DA.to_schema_name(username=DA.username, course_code=DA.course_config.course_code, lesson_name=DA.lesson_config.name) 

spark.conf.set("DA.my_schema_name", DA.my_schema_name)
spark.conf.set("da.my_schema_name", DA.my_schema_name)

# Get fully qualified user-specific schema name (hive_metastore.<my_schema_name>)
hive_user_schema = f"hive_metastore.{DA.my_schema_name}"

# Create or reset user-specific schema in hive_metastore
spark.sql(f"DROP DATABASE IF EXISTS {hive_user_schema} CASCADE")
spark.sql(f"CREATE DATABASE {hive_user_schema}")

# Create movies table in the user-specific schema in the hive metastore
(spark.read
      .option("header", True)
      .csv(f"{DA.paths.datasets}/movie_ratings/movies.csv")
      .write
      .mode("overwrite")
      .saveAsTable(f"{hive_user_schema}.movies"))

# Show predefined tables
print(f"Predefined tables in \"{hive_user_schema}\":")
tables = spark.sql(f"SHOW TABLES IN {hive_user_schema}").filter("isTemporary == false").select("tableName").collect()
if len(tables) == 0: print("  -none-")
for row in tables: print(f"  {row[0]}")

