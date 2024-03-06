-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Report-I: Find the Dominant Drivers of All the Time

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### BI Reporting Requirements 
-- MAGIC
-- MAGIC - Provide a comprehensive overview of Formula 1 racing, including historical insights and recent trends.
-- MAGIC - Uncover patterns and correlations within the data to better understand the factors influencing race outcomes i.e. dominant drivers, dominant teams
-- MAGIC - Create interactive Databricks visualizations and dashboards to showcase our findings and engage with the Formula 1 community.
-- MAGIC - Explore opportunities for further research and analysis in the field of motorsport analytics.
-- MAGIC
-- MAGIC ##### VI. Scheduling Requirements 
-- MAGIC
-- MAGIC - Schedule to run every Sunday 10PM
-- MAGIC - Ability to monitor pipelines
-- MAGIC - Ability to re-run failed pipelines
-- MAGIC - Ability to set-up alerts on failures
-- MAGIC
-- MAGIC
-- MAGIC ##### V. Misc Requirements 
-- MAGIC
-- MAGIC - Ability to see history and time travel
-- MAGIC - Ability to roll back to a previous version 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create races_results FACT table <br>
-- MAGIC granularity : race_year, driver, team_name

-- COMMAND ----------


-- There is an anomaly in the data : points never stayed the same. Based on years it changed: to stabilize this I wanna give 10 points to the position 1 (11-1=10)

CREATE TABLE IF NOT EXISTS formuladl101_presentation.race_results_fct
USING parquet
AS
SELECT 
        races.race_year,
        constructors.name as team_name, 
        drivers.name as driver_name,
        results.position,
        results.points,
        11 - results.position as calculated_points
  FROM formuladl101_processed.results
  JOIN formuladl101_processed.drivers ON (results.driver_id = drivers.driver_id)
  JOIN formuladl101_processed.constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN formuladl101_processed.races ON (results.race_id = races.race_id)
WHERE results.position <=10

-- COMMAND ----------

SELECT * FROM formuladl101_presentation.race_results_fct

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Find the dominant drivers of all times <br>

-- COMMAND ----------

SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
  FROM formuladl101_presentation.race_results_fct
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Find the dominant drivers of the last decade <br>

-- COMMAND ----------

SELECT 
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
FROM formuladl101_presentation.race_results_fct
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Find the dominant drivers years between 2001 and 2010 <br>

-- COMMAND ----------

SELECT 
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
FROM formuladl101_presentation.race_results_fct
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------


