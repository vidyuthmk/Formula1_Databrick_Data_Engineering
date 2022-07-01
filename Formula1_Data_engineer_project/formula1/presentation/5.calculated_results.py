# Databricks notebook source
# MAGIC %md
# MAGIC ### Calcualated results

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21","File Date")
file_date=dbutils.widgets.get("file_date")

# COMMAND ----------

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS F1_PRESENTATION.CALCULATED_RACE_RESULTS
            (
            RACE_YEAR INT,
            TEAM_NAME STRING,
            DRIVER_ID INT,
            DRIVER_NAME STRING,
            RACE_ID INT,
            POSITION INT,
            POINTS INT,
            CALCULATED_POINTS INT,
            CREATED_DATE TIMESTAMP,
            UPDATED_DATE TIMESTAMP
            )
            USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW CALCULATED_POINTS
AS
SELECT RC.RACE_YEAR AS YEAR,
R.FILE_DATE,
D.NAME AS DRIVER_NAME,
D.DRIVER_ID,
RC.RACE_ID,
C.NAME AS TEAM,
R.POSITION AS POSITION,
R.POINTS AS POINTS,
11-R.POSITION AS CALCULATED_POINTS
FROM F1_PROCESSED.RESULTS AS R
JOIN F1_PROCESSED.DRIVERS D ON R.DRIVER_ID = D.DRIVER_ID
JOIN F1_PROCESSED.CONSTRUCTORS C ON C.CONSTRUCTOR_ID = R.CONSTRUCTOR_ID
JOIN F1_PROCESSED.RACES RC ON RC.RACE_ID=R.RACE_ID
WHERE R.POSITION <=10 AND R.FILE_DATE = '{file_date}'
""")

# COMMAND ----------

spark.sql(f"""
MERGE INTO F1_PRESENTATION.CALCULATED_RACE_RESULTS T
USING CALCULATED_POINTS S
ON T.RACE_ID=S.RACE_ID AND T.DRIVER_ID = S.DRIVER_ID
WHEN MATCHED THEN
UPDATE SET 
            POSITION =S.POSITION,
            POINTS = S.POINTS,
            CALCULATED_POINTS =S.CALCULATED_POINTS,
            UPDATED_DATE =CURRENT_TIMESTAMP
WHEN NOT MATCHED 
THEN INSERT (
            RACE_YEAR ,
            TEAM_NAME ,
            DRIVER_ID ,
            DRIVER_NAME ,
            RACE_ID ,
            POSITION ,
            POINTS ,
            CALCULATED_POINTS ,
            CREATED_DATE 
             )
      VALUES(
            S.YEAR ,
            S.TEAM ,
            S.DRIVER_ID ,
            S.DRIVER_NAME ,
            S.RACE_ID ,
            S.POSITION ,
            S.POINTS ,
            S.CALCULATED_POINTS ,
            CURRENT_TIMESTAMP 
      )
""")

# COMMAND ----------


