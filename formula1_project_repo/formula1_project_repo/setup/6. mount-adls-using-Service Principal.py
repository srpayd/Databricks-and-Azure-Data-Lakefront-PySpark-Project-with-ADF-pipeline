# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake using Service Principal
# MAGIC
# MAGIC ##Steps to follow
# MAGIC - 1. Get client_id, tenant_id and clint_secret from key vault
# MAGIC - 2. Set Spark Config with App_id & secret
# MAGIC - 3. Call file system 
# MAGIC - 4. Explore

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formuladl101-secret-scope')

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formuladl101-secret-scope', key = 'formuladl101-client-id')
tenant_id = dbutils.secrets.get(scope = 'formuladl101-secret-scope', key = 'formuladl101-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formuladl101-secret-scope', key = 'formuladl101-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formuladl101.dfs.core.windows.net/",
  mount_point = "/mnt/formuladl101/demo",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@formuladl101.dfs.core.windows.net/",
  mount_point = "/mnt/formuladl101/raw",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://stage@formuladl101.dfs.core.windows.net/",
  mount_point = "/mnt/formuladl101/stage",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://presentation@formuladl101.dfs.core.windows.net/",
  mount_point = "/mnt/formuladl101/presentation",
  extra_configs = configs)

# COMMAND ----------

# Step 4 : Assign "Storage Blob Data Contributor" Role to Service Principal. So if Databricks can reach Service Principal, it already has full permission to Azure Data Lake Storage.

# Go to storage you will access via Databricks. 
# Click Access Control > Add > Storage Blob Data Contributor > Next > Select Members > Type name of the service principal you created > Create.

display(dbutils.fs.ls("/mnt/formuladl101/demo"))



# COMMAND ----------

display(dbutils.fs.ls("/mnt/formuladl101/raw"))

# COMMAND ----------

display(spark.read.csv("/mnt/formuladl101/raw"))

# COMMAND ----------


