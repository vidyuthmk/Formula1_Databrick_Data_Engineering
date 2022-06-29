-- Databricks notebook source
SELECT CURRENT_DATABASE()

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

USE F1_PROCESSED

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

REFRESH TABLE F1_PROCESSED.RESULTS

-- COMMAND ----------

SELECT RC.RACE_YEAR AS YEAR,
D.NAME AS DRIVER_NAME,
C.NAME AS TEAM,
COUNT (CASE WHEN R.POSITION = 1 THEN 1 END ) AS WINS,
SUM(R.POINTS) AS POINTS,
RANK() OVER(PARTITION BY RC.RACE_YEAR ORDER BY SUM(POINTS) DESC) AS STANDINGS
FROM F1_PROCESSED.RESULTS AS R
JOIN F1_PROCESSED.DRIVERS D ON R.DRIVER_ID = D.DRIVER_ID
JOIN F1_PROCESSED.CONSTRUCTORS C ON C.CONSTRUCTOR_ID = R.CONSTRUCTOR_ID
JOIN F1_PROCESSED.RACES RC ON RC.RACE_ID=R.RACE_ID
GROUP BY YEAR,DRIVER_NAME,TEAM
ORDER BY POINTS DESC

-- COMMAND ----------

DROP TABLE F1_PRESENTATION.CALCULATED_RACE_RESULT;
CREATE TABLE F1_PRESENTATION.CALCULATED_RACE_RESULT
USING PARQUET
AS
SELECT RC.RACE_YEAR AS YEAR,
D.NAME AS DRIVER_NAME,
C.NAME AS TEAM,
R.POSITION AS POSITION,
R.POINTS AS POINTS,
11-R.POSITION AS CALCULATED_POINTS
FROM F1_PROCESSED.RESULTS AS R
JOIN F1_PROCESSED.DRIVERS D ON R.DRIVER_ID = D.DRIVER_ID
JOIN F1_PROCESSED.CONSTRUCTORS C ON C.CONSTRUCTOR_ID = R.CONSTRUCTOR_ID
JOIN F1_PROCESSED.RACES RC ON RC.RACE_ID=R.RACE_ID
WHERE R.POSITION <=10


-- COMMAND ----------

SELECT * FROM F1_PRESENTATION.CALCULATED_RACE_RESULT

-- COMMAND ----------

