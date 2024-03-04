# Databricks notebook source
# List DBFS Root
display(dbutils.fs.ls('/'))

# COMMAND ----------

# MAGIC %md 
# MAGIC When you create Databricks workspace, it is not default enables to store the files in DBFS automatically. We can enable this.
# MAGIC
# MAGIC **Admin Console > Workspace Settings > DBFS File Browser enable it.**

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

# MAGIC %md 
# MAGIC To see the content of the files under DBFS File you can use **Spark DataFrame Reader API**

# COMMAND ----------


