# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config in the cluster
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

# Let's list prettier 
display(dbutils.fs.ls("abfss://demo@formuladl101.dfs.core.windows.net"))

# COMMAND ----------

# This returns spark dataframe
display(spark.read.csv("abfss://demo@formuladl101.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC However this way we exposed the access key public.
# MAGIC Also, anyone has this access key has full privilege. Let's do 3 things:
# MAGIC - Embed access key (SAS token)
# MAGIC - Restrict the privilege of users to only Read (SAS token)
# MAGIC - Provide access only limited amount of time (SAS token)

# COMMAND ----------


