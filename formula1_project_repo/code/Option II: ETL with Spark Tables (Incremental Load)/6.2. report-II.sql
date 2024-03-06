-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Report-II: Find the Dominant Teams of All the Time

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
-- MAGIC ### Find the dominant teams of all times <br>

-- COMMAND ----------

SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
  FROM formuladl101_presentation.race_results_fct
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Find the dominant teams of the last decade <br>

-- COMMAND ----------

SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
  FROM formuladl101_presentation.race_results_fct
 WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Find the dominant teams years between 2001 and 2010 <br>

-- COMMAND ----------

SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
  FROM formuladl101_presentation.race_results_fct
 WHERE race_year BETWEEN 2001 AND 2011
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------


