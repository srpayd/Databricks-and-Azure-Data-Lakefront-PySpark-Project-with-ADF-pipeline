-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Visualization of Report-I

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### BI Reporting Requirements 
-- MAGIC
-- MAGIC - Provide a comprehensive overview of Formula 1 racing, including historical insights and recent trends.
-- MAGIC - Uncover patterns and correlations within the data to better understand the factors influencing race outcomes i.e. dominant drivers, dominant teams
-- MAGIC - Create interactive Databricks visualizations and dashboards to showcase our findings and engage with the Formula 1 community.
-- MAGIC - Explore opportunities for further research and analysis in the field of motorsport analytics.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Let's rank the dominant drivers based on avg_point <br>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vw_dominant_drivers
AS
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points,
       RANK() OVER(order by ROUND(AVG(calculated_points),2) DESC) AS driver_rank
  FROM formuladl101_presentation.race_results_fct
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Line Chart : Let's see TOP 10 drivers over the years <br>

-- COMMAND ----------

SELECT race_year, 
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
  FROM formuladl101_presentation.race_results_fct
 WHERE driver_name IN (SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Bar Chart 

-- COMMAND ----------

SELECT race_year, 
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
  FROM formuladl101_presentation.race_results_fct
 WHERE driver_name IN (SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Findings:**
-- MAGIC
-- MAGIC - The chart includes 16 drivers, who are likely among the top performers based on their ranking in a subquery list of dominant drivers.
-- MAGIC - The driver with the highest total points on this chart has significantly more than the others, while the rest have a more clustered distribution of total points.
-- MAGIC - The bar chart does not show the race_year, so it is likely aggregating the total points over multiple years or the entirety of each driver's career within the dataset.
-- MAGIC - There are a few drivers with total points approaching or exceeding 600, indicating very successful careers.
-- MAGIC - The majority of the drivers have total points ranging between 200 and 400.
-- MAGIC - The driver with the lowest total points among the ones shown still has a significant number of points, likely over 100, which implies that even the drivers with fewer points compared to their peers are still high performers.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Area Chart 

-- COMMAND ----------

SELECT race_year, 
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
  FROM formuladl101_presentation.race_results_fct
 WHERE driver_name IN (SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Findings:**
-- MAGIC
-- MAGIC - The chart covers a timespan from the early 1970s to just before 2020.
-- MAGIC - Each driver's performance varies from year to year, with peaks and troughs indicating seasons of high and low average points.
-- MAGIC - Drivers like Michael Schumacher and Lewis Hamilton show periods of sustained high performance, as indicated by the wider and taller areas under their lines.
-- MAGIC - Some drivers, such as Niki Lauda and Ayrton Senna, also have prominent peaks, suggesting seasons where they averaged a high number of points per race.
-- MAGIC - There's a noticeable trend where drivers from different eras have their periods of peak performance. For example, Jackie Stewart's performance is prominent in the early part of the chart (1970s), while drivers like Sebastian Vettel and Lewis Hamilton have their peaks in the more recent part of the chart (2000s and 2010s).
-- MAGIC - The chart indicates that the performance of drivers can fluctuate significantly from year to year, which could be due to a variety of factors including changes in team performance, regulations, and personal circumstances.
