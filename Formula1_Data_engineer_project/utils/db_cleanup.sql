-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### clean up the databases for processed and presentation folder

-- COMMAND ----------

DROP DATABASE IF EXISTS F1_PRESENTATION CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS F1_PRESENTATION
LOCATION "/mnt/formula1newdatalake/presentation"

-- COMMAND ----------

DROP DATABASE IF EXISTS F1_PROCESSED CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS F1_PROCESSED 
LOCATION "/mnt/formula1newdatalake/processed"

-- COMMAND ----------

DROP TABLE IF EXISTS f1_processed.results

-- COMMAND ----------

drop table if exists f1_presentation.driver_standings

-- COMMAND ----------

drop table if exists f1_presentation.constructor_standing


-- COMMAND ----------

DROP DATABASE IF EXISTS formula1 CASCADE;

-- COMMAND ----------


