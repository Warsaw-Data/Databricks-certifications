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

DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"

DA.conclude_setup()

# COMMAND ----------

def print_sql(rows, sql):
    html = f"<textarea style=\"width:100%\" rows={rows}> \n{sql.strip()}</textarea>"
    displayHTML(html)

# COMMAND ----------

def _generate_config():
    print_sql(33, f"""
CREATE DATABASE IF NOT EXISTS {DA.schema_name}
LOCATION '{DA.paths.working_dir}';

USE {DA.schema_name};

CREATE TABLE user_ping 
(user_id STRING, ping INTEGER, time TIMESTAMP); 

CREATE TABLE user_ids (user_id STRING);

INSERT INTO user_ids VALUES
("potato_luver"),
("beanbag_lyfe"),
("default_username"),
("the_king"),
("n00b"),
("frodo"),
("data_the_kid"),
("el_matador"),
("the_wiz");

CREATE FUNCTION get_ping()
    RETURNS INT
    RETURN int(rand() * 250);
    
CREATE FUNCTION is_active()
    RETURNS BOOLEAN
    RETURN CASE 
        WHEN rand() > .25 THEN true
        ELSE false
        END;
""")
    
DA.generate_config = _generate_config    

# COMMAND ----------

def _generate_load():
    print_sql(12, f"""
USE {DA.schema_name};

INSERT INTO user_ping
SELECT *, 
  get_ping() ping, 
  current_timestamp() time
FROM user_ids
WHERE is_active()=true;

SELECT * FROM user_ping;
""")

DA.generate_load = _generate_load

# COMMAND ----------

def _generate_user_counts():
    print_sql(10, f"""
USE {DA.schema_name};

SELECT user_id, count(*) total_records
FROM user_ping
GROUP BY user_id
ORDER BY 
  total_records DESC,
  user_id ASC;
""")

DA.generate_user_counts = _generate_user_counts

# COMMAND ----------

def _generate_avg_ping():
    print_sql(10, f"""
USE {DA.schema_name};

SELECT user_id, window.end end_time, mean(ping) avg_ping
FROM user_ping
GROUP BY user_id, window(time, '3 minutes')
ORDER BY
  end_time DESC,
  user_id ASC;
""")

DA.generate_avg_ping = _generate_avg_ping

# COMMAND ----------

def _generate_summary():
    print_sql(8, f"""
USE {DA.schema_name};

SELECT user_id, min(time) first_seen, max(time) last_seen, count(*) total_records, avg(ping) total_avg_ping
FROM user_ping
GROUP BY user_id
ORDER BY user_id ASC;
""")

DA.generate_summary = _generate_summary

