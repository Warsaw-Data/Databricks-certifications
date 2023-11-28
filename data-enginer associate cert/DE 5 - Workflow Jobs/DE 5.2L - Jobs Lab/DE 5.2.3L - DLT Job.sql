-- Databricks notebook source
-- MAGIC %run ../Includes/Classroom-Setup-05.2.3L

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE recordings_bronze
AS SELECT current_timestamp() receipt_time, input_file_name() source_file, *
  FROM cloud_files("${source}", "json", map("cloudFiles.schemaHints", "time DOUBLE"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE pii
AS SELECT *
  FROM cloud_files("${datasets_path}/healthcare/patient", "csv", map("header", "true", "cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE recordings_enriched
  (CONSTRAINT positive_heartrate EXPECT (heartrate > 0) ON VIOLATION DROP ROW)
AS SELECT 
  CAST(a.device_id AS INTEGER) device_id, 
  CAST(a.mrn AS LONG) mrn, 
  CAST(a.heartrate AS DOUBLE) heartrate, 
  CAST(from_unixtime(a.time, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time,
  b.name
  FROM STREAM(live.recordings_bronze) a
  INNER JOIN STREAM(live.pii) b
  ON a.mrn = b.mrn

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE daily_patient_avg
  COMMENT "Daily mean heartrates by patient"
AS SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE(time) `date`
  FROM STREAM(live.recordings_enriched)
  GROUP BY mrn, name, DATE(time)
