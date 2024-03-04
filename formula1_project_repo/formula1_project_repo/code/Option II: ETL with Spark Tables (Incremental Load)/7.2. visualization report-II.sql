-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Visualization of Report-II

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### BI Reporting Requirements 
-- MAGIC
-- MAGIC - Provide a comprehensive overview of Formula 1 racing, including historical insights and recent trends.
-- MAGIC - Uncover patterns and correlations within the data to better understand the factors influencing race outcomes i.e. dominant drivers, dominant teams
-- MAGIC - Create interactive Databricks visualizations and dashboards to showcase our findings and engage with the Formula 1 community.
-- MAGIC - Explore opportunities for further research and analysis in the field of motorsport analytics.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Let's rank the dominant teams based on avg_point <br>

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vw_dominant_teams
AS
SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
  FROM formuladl101_presentation.race_results_fct
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Let's see TOP 5 teams over the years <br>

-- COMMAND ----------

SELECT race_year, 
       team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
  FROM formuladl101_presentation.race_results_fct
 WHERE team_name IN (SELECT team_name FROM vw_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Bar Chart 

-- COMMAND ----------

SELECT race_year, 
       team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
  FROM formuladl101_presentation.race_results_fct
 WHERE team_name IN (SELECT team_name FROM vw_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Area Chart 

-- COMMAND ----------

SELECT race_year, 
       team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
  FROM formuladl101_presentation.race_results_fct
 WHERE team_name IN (SELECT team_name FROM vw_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Findings:**
-- MAGIC
-- MAGIC - There are five teams included in this analysis: Ferrari, McLaren, Mercedes, Red Bull, and Williams. These are likely the teams that were ranked in the top 5 at the time of the data extraction.
-- MAGIC - The data spans from 1950 to just before 2020, with some fluctuations in the number of races or points.
-- MAGIC - Ferrari and McLaren seem to have a long history in Formula 1, with data going back to the 1950s.
-- MAGIC - Mercedes and Red Bull appear to have data starting from around 2010, suggesting they may be newer teams or only recently became part of the top teams.
-- MAGIC - Williams also has a long history but seems to have had fewer points on average compared to Ferrari and McLaren, particularly in recent years.
-- MAGIC - There are periods where certain teams dominated. For example, there's a period in the 1980s where McLaren appears to be the top performer, and another in the early 2000s for Ferrari.
-- MAGIC - In the most recent years, there is a steep decline in the average points for Williams, indicating a drop in performance.
-- MAGIC - The average points for all teams show significant variance from year to year, indicating the competitive nature of the sport and the changes in team performances.
-- MAGIC - In the latest years shown, Mercedes and Red Bull seem to be performing better on average compared to the other teams, possibly indicating a shift in the competitive landscape.

-- COMMAND ----------


