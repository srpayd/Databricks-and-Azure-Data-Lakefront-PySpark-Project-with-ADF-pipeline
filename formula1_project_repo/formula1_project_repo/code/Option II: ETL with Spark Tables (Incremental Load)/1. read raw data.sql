-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## We will read raw data from Azure Data Lake Storage via External tables 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formuladl101_raw

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS formuladl101_raw.circuits;
CREATE TABLE IF NOT EXISTS formuladl101_raw.circuits(
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/formuladl101/raw/circuits.csv", header true)


-- COMMAND ----------

SELECT * FROM formuladl101_raw.circuits

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS formuladl101_raw.races;
CREATE TABLE IF NOT EXISTS formuladl101_raw.races(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
OPTIONS (path "/mnt/formuladl101/raw/circuits.csv", header true)


-- COMMAND ----------

-- COMMAND ----------

SELECT * FROM formuladl101_raw.races;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create constructors table

-- COMMAND ----------

DROP TABLE IF EXISTS formuladl101_raw.constructors;
CREATE TABLE IF NOT EXISTS formuladl101_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/formuladl101/raw/constructors.json")


-- COMMAND ----------

SELECT * FROM formuladl101_raw.constructors;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create drivers table

-- COMMAND ----------

DROP TABLE IF EXISTS formuladl101_raw.drivers;
CREATE TABLE IF NOT EXISTS formuladl101_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS (path "/mnt/formuladl101/raw/drivers.json")


-- COMMAND ----------

SELECT * FROM formuladl101_raw.drivers

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create results table

-- COMMAND ----------

DROP TABLE IF EXISTS formuladl101_raw.results;
CREATE TABLE IF NOT EXISTS formuladl101_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "/mnt/formuladl101/raw/results.json")


-- COMMAND ----------

SELECT * FROM formuladl101_raw.results

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create pit_stops table

-- COMMAND ----------

DROP TABLE IF EXISTS formuladl101_raw.pit_stops;
CREATE TABLE IF NOT EXISTS formuladl101_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "/mnt/formuladl101/raw/pit_stops.json", multiLine true)


-- COMMAND ----------


SELECT * FROM formuladl101_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create lap_times table

-- COMMAND ----------

DROP TABLE IF EXISTS formuladl101_raw.lap_times;
CREATE TABLE IF NOT EXISTS formuladl101_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formuladl101/raw/lap_times")

-- COMMAND ----------

SELECT * FROM formuladl101_raw.lap_times


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create qualifying table

-- COMMAND ----------

DROP TABLE IF EXISTS formuladl101_raw.qualifying;
CREATE TABLE IF NOT EXISTS formuladl101_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "/mnt/formuladl101/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM formuladl101_raw.qualifying

-- COMMAND ----------

DESC EXTENDED formuladl101_raw.qualifying;

-- COMMAND ----------


