-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create processed database and managed tables to manage them within DBFS

-- COMMAND ----------

-- If we dont specift the LOCATION data will be creating under default database 
CREATE DATABASE IF NOT EXISTS formuladl101_processed
LOCATION "/mnt/formuladl101/stage"

-- COMMAND ----------

DESC DATABASE formuladl101_raw

-- COMMAND ----------

DESC DATABASE formuladl101_processed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC DB with External Tables (**metadata**): dbfs:/user/hive/warehouse/formuladl101_raw.db     <br> 
-- MAGIC DB with Managed Tables (**metadata + dbfs storage**): dbfs:/mnt/formuladl101/processed 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Read external table 
-- MAGIC circuits_df = spark.table("formuladl101_raw.circuits")
-- MAGIC
-- MAGIC ## Write as managed table 
-- MAGIC circuits_df.write.mode("overwrite").format("parquet").saveAsTable("formuladl101_processed.circuits")
-- MAGIC

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Read external table 
-- MAGIC races_selected_df = spark.table("formuladl101_raw.races")
-- MAGIC
-- MAGIC # ## Write as managed table 
-- MAGIC races_selected_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("formuladl101_processed.races")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Read external table 
-- MAGIC circuits_df = spark.table("formuladl101_raw.circuits")
-- MAGIC
-- MAGIC ## Write as managed table 
-- MAGIC circuits_df.write.mode("overwrite").format("parquet").saveAsTable("formuladl101_processed.circuits")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Read external table 
-- MAGIC circuits_df = spark.table("formuladl101_raw.circuits")
-- MAGIC
-- MAGIC ## Write as managed table 
-- MAGIC circuits_df.write.mode("overwrite").format("parquet").saveAsTable("formuladl101_processed.circuits")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Read external table 
-- MAGIC circuits_df = spark.table("formuladl101_raw.circuits")
-- MAGIC
-- MAGIC ## Write as managed table 
-- MAGIC circuits_df.write.mode("overwrite").format("parquet").saveAsTable("formuladl101_processed.circuits")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Read external table 
-- MAGIC circuits_df = spark.table("formuladl101_raw.circuits")
-- MAGIC
-- MAGIC ## Write as managed table 
-- MAGIC circuits_df.write.mode("overwrite").format("parquet").saveAsTable("formuladl101_processed.circuits")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Read external table 
-- MAGIC circuits_df = spark.table("formuladl101_raw.circuits")
-- MAGIC
-- MAGIC ## Write as managed table 
-- MAGIC circuits_df.write.mode("overwrite").format("parquet").saveAsTable("formuladl101_processed.circuits")
-- MAGIC
