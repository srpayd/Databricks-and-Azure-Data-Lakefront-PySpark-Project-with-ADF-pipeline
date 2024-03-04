# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformation-II: driver_standings

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Transfromation Requirements 
# MAGIC
# MAGIC - Join the key information required for reporting to create a new table
# MAGIC - Join the key information required for Analysis to create a new table
# MAGIC - Transformed table must have audit columns 
# MAGIC - Must be able to analyze the transformed data viw SQL
# MAGIC - Transformed data must be stored in columnar format
# MAGIC - Transformation logic must be able to handle incremental load
# MAGIC <br>
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../../includes/config"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Reading all files & join into a single table 

# COMMAND ----------

race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results") \
                              .where(f"file_date = '{v_file_date}'") \
                              .select("race_year") \
                              .distinct() \
                              .collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
                              .where(col("race_year").isin(race_year_list)) 

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count , countDistinct

driver_standing_df = race_results_df.groupBy(race_results_df["race_year"], 
                                             race_results_df["driver_name"],
                                             race_results_df["driver_nationality"], 
                                             race_results_df["team"]) \
                                    .agg(sum(race_results_df["points"]).alias("total_points"), 
                                         count(when(race_results_df["position"]==1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

overwrite_partition(final_df, 'formuladl101_presentation', 'driver_standings', 'race_year')

# COMMAND ----------

dbutils.notebook.exit('Success')
