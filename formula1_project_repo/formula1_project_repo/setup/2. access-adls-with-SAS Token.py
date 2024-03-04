# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC You can limit access based on **user, source, permission, time longer**  with token. <br> Here are the steps:
# MAGIC - 1. spark.confs
# MAGIC - 2. list files
# MAGIC - 3. read data

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formuladl101.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formuladl101.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set('fs.azure.sas.fixed.token.formuladl101.dfs.core.windows.net', '?sv=2023-01-03&st=2024-02-25T21%3A13%3A46Z&se=2024-02-26T21%3A13%3A46Z&sr=c&sp=rl&sig=AEoYJ0W6M4dJykCosZOR2gUyUKokIhDrhE5pGnfU608%3D')

# Let's list prettier 
display(dbutils.fs.ls("abfss://demo@formuladl101.dfs.core.windows.net"))


# COMMAND ----------


