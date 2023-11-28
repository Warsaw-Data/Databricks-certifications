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

import os, time, shutil, sqlite3
import pandas as pd

# Create a user-specific copy of the sales-csv.
DA.paths.sales_csv = f"{DA.paths.working_dir}/sales-csv"
dbutils.fs.cp(f"{DA.paths.datasets}/ecommerce/raw/sales-csv", DA.paths.sales_csv, True)

start = int(time.time())
print(f"Creating the users table", end="...")

DA.paths.ecommerce_db = f"{DA.paths.working_dir}/ecommerce.db"
datasource_path = f"{DA.paths.datasets}/ecommerce/raw/users-historical"

# Create the temp directory and declare the path to the temp db file.
db_temp_dir = f"/tmp/{DA.username}"
dbutils.fs.mkdirs(f"file:{db_temp_dir}")
db_temp_path = f"{db_temp_dir}/ecommerce.db"

# Spark => JDBC cannot create the database reliably but Pandas can.
conn = sqlite3.connect(db_temp_path) 
c = conn.cursor()
c.execute('CREATE TABLE IF NOT EXISTS users (user_id string, user_first_touch_timestamp decimal(20,0), email string)')
conn.commit()
df = pd.read_parquet(path = datasource_path.replace("dbfs:/", '/dbfs/'))
df.to_sql('users', conn, if_exists='replace', index = False)

# Move the temp db to the final location
dbutils.fs.mv(f"file:{db_temp_path}", DA.paths.ecommerce_db)
DA.paths.ecommerce_db = DA.paths.ecommerce_db.replace("dbfs:/", "/dbfs/")

# Report on the setup time.
total = spark.read.parquet(datasource_path).count()
print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

DA.conclude_setup()
