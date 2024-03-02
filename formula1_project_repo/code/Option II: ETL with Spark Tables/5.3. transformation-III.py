# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformation-III: constructors_standings

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

from pyspark.sql.functions import sum, when, count, col

constructor_standings_df = race_results_df.groupBy("race_year", "team") \
                                          .agg(sum("points").alias("total_points"),count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("formuladl101_presentation.constructor_standings")

# COMMAND ----------

dbutils.notebook.exit('Success')
