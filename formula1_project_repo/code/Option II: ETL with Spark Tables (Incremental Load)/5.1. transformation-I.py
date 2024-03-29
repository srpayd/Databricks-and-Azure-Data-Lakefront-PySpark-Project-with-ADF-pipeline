# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformation-I: race_results

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

drivers_df = spark.read.parquet(f"{stage_folder_path}/drivers") \
                       .withColumnRenamed("number", "driver_number") \
                       .withColumnRenamed("name", "driver_name") \
                       .withColumnRenamed("nationality", "driver_nationality") 


# COMMAND ----------

constructors_df = spark.read.parquet(f"{stage_folder_path}/constructors") \
.withColumnRenamed("name", "team") 


# COMMAND ----------

circuits_df = spark.read.parquet(f"{stage_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 


# COMMAND ----------

races_df = spark.read.parquet(f"{stage_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

results_df = spark.read.parquet(f"{stage_folder_path}/results") \
                        .where(f"file_date = '{v_file_date}'") \
                        .withColumnRenamed("time", "race_time") \
                        .withColumnRenamed("race_id", "result_race_id") \
                        .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### a. Join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
                           .select(races_df.race_id, 
                                   races_df.race_year, 
                                   races_df.race_name, 
                                   races_df.race_date, 
                                   circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC #### b. Join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = race_results_df.select("race_year", 
                                  "race_name", 
                                  "race_date", 
                                  "circuit_location",        
                                  "driver_name", 
                                  "driver_number", 
                                  "driver_nationality",
                                  "team", 
                                  "grid", 
                                  "fastest_lap", 
                                  "race_time", 
                                  "points", 
                                  "position",
                                  "result_file_date",
                                  "race_id") \
                          .withColumn("created_date", current_timestamp()) \
                          .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

display(final_df.filter((final_df["race_year"] == 2020) & (final_df["race_name"] == 'Abu Dhabi Grand Prix')) \
                .orderBy(final_df.points.desc()))


# COMMAND ----------

overwrite_partition(final_df, 'formuladl101_presentation', 'race_results', 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')
