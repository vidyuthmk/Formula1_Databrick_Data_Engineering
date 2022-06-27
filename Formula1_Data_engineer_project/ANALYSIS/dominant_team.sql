-- Databricks notebook source
  SELECT TEAM,
  COUNT(1) AS NO_OF_RACES,
  SUM(DC.CALCULATED_POINTS) AS TOTAL_POINTS,
  ROUND(AVG(DC.CALCULATED_POINTS),2) AS AVG_POINTS
  FROM f1_presentation.calculated_race_result DC
  GROUP BY TEAM
  HAVING   COUNT(1) > 100
  ORDER BY AVG_POINTS DESC

-- COMMAND ----------

  SELECT TEAM,
  COUNT(1) AS NO_OF_RACES,
  SUM(DC.CALCULATED_POINTS) AS TOTAL_POINTS,
  ROUND(AVG(DC.CALCULATED_POINTS),2) AS AVG_POINTS
  FROM f1_presentation.calculated_race_result DC
  WHERE DC.YEAR BETWEEN 2011 AND 2020
  GROUP BY TEAM
  HAVING   COUNT(1) > 100
  ORDER BY AVG_POINTS DESC

-- COMMAND ----------

  SELECT TEAM,
  COUNT(1) AS NO_OF_RACES,
  SUM(DC.CALCULATED_POINTS) AS TOTAL_POINTS,
  ROUND(AVG(DC.CALCULATED_POINTS),2) AS AVG_POINTS
  FROM f1_presentation.calculated_race_result DC
  WHERE DC.YEAR BETWEEN 2001 AND 2010
  GROUP BY TEAM
  HAVING   COUNT(1) > 100
  ORDER BY AVG_POINTS DESC

-- COMMAND ----------


