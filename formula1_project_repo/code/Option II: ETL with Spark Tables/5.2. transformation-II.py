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

# MAGIC %run "../../../includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Reading all files & join into a single table 

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") 

# COMMAND ----------

from pyspark.sql.functions import sum, when, count , countDistinct

driver_standing_df = race_results_df.groupBy(race_results_df["race_year"], 
                                             race_results_df["driver_name"],
                                             race_results_df["driver_nationality"], 
                                             race_results_df["team"]) \
                                    .agg(sum(race_results_df["points"]).alias("total_points"), 
                                         count(when(race_results_df["position"]==1, True)).alias("wins"))

# COMMAND ----------

display(driver_standing_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("formuladl101_presentation.driver_standings")

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------


