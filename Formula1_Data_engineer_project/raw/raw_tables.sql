-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS FORMULA1

-- COMMAND ----------

USE FORMULA1


-- COMMAND ----------

drop table if exists FORMULA1.CIRCUITS;
CREATE TABLE IF NOT EXISTS FORMULA1.CIRCUITS(circuitId INT,
circuitRef string,
name string,
location string,
country string,
lat double,
lng double,
alt double,
url string
)
USING CSV  
options (path "/mnt/formula1newdatalake/raw/circuits.csv", HEADER "TRUE")

-- COMMAND ----------

select * from FORMULA1.CIRCUITS

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS FORMULA1.RACE(raceid int,
year int,
round int,
circuitid int,
name string,
date date,
time string,
url string
)
USING CSV  
options (path "/mnt/formula1newdatalake/raw/races.csv", HEADER "TRUE")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS FORMULA1.constructor(constructorId int,
constructorRef string,
name string,
nationality string,
url string
)
USING JSON  
options (path "/mnt/formula1newdatalake/raw/constructors.json", HEADER "TRUE")

-- COMMAND ----------

SELECT * FROM FORMULA1.CONSTRUCTOR

-- COMMAND ----------

DROP TABLE IF EXISTS FORMULA1.DRIVERS;
CREATE TABLE IF NOT EXISTS FORMULA1.DRIVERS(code string,
dob date,
driverId int,
driverRef string,
name STRUCT<forename string, surname string>,
nationality string,
number int,
url string
)
USING JSON  
options (path "/mnt/formula1newdatalake/raw/drivers.json", HEADER "TRUE")

-- COMMAND ----------

SELECT * FROM FORMULA1.DRIVERS

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS FORMULA1.RESULT(constructorId int,
driverId int,
fastestLap int,
fastestLapSpeed string,
fastestLapTime string,
grid int,
laps int,
milliseconds int,
number int,
points float,
position int,
positionOrder int,
positionText string,
raceId int,
rank int,
resultId int,
time string,
statusId int
)
USING JSON  
options (path "/mnt/formula1newdatalake/raw/results.json", HEADER "TRUE")

-- COMMAND ----------

SELECT * FROM FORMULA1.RESULT

-- COMMAND ----------

DROP TABLE IF EXISTS FORMULA1.PIT_STOPS;
CREATE TABLE IF NOT EXISTS FORMULA1.PIT_STOPS(raceId INT,
driverId INT,
stop INT,
lap INT,
time string,
duration string,
milliseconds INT
)
USING JSON  
options (path "/mnt/formula1newdatalake/raw/pit_stops.json", HEADER "TRUE", MULTILINE "TRUE")

-- COMMAND ----------

SELECT * FROM FORMULA1.PIT_STOPS

-- COMMAND ----------

DROP TABLE IF EXISTS FORMULA1.LAP_TIME;
CREATE TABLE IF NOT EXISTS FORMULA1.LAP_TIME(raceId INT,
driverId INT,
position INT,
lap INT,
time STRING,
milliseconds INT
)
USING CSV  
options (path "/mnt/formula1newdatalake/raw/lap_times", HEADER "TRUE", MULTILINE "TRUE")

-- COMMAND ----------

SELECT * FROM FORMULA1.LAP_TIME

-- COMMAND ----------

drop table if exists FORMULA1.QUALIFYING;
CREATE TABLE IF NOT EXISTS FORMULA1.QUALIFYING(raceId INT,
qualifyId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 string,
q2 string,
q3 string
)
USING JSON  
options (path "/mnt/formula1newdatalake/raw/qualifying", HEADER "TRUE", MULTILINE "TRUE")

-- COMMAND ----------

SELECT * FROM FORMULA1.QUALIFYING

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS FORMULA1_PROCESSED

-- COMMAND ----------

USE FORMULA1_PROCESSED

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS FORMULA1_PROCESSED.CIRCUITS 
AS
SELECT 8 FROM FORMULA1.CIRCUITS

-- COMMAND ----------


