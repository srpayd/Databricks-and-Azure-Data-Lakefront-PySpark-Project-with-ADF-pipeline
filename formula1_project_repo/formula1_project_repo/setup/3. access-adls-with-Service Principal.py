# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using Service Principal
# MAGIC
# MAGIC ##Steps to follow
# MAGIC - 1. Register for Azure AD Application / Service Principal
# MAGIC - 2. Generate a secret/passsword for the Application
# MAGIC - 3. Set Spark Config with App_id & secret
# MAGIC - 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

# Step 1 : We created a Service Principal : Azure Active Directory > App Registrations > (Name a service principal) > Create
client_id = "7d881e88-f128-457e-839d-e479e728c25b"
tenant_id = "55783ac4-7fd1-4c0c-b42f-252c080a63f0"

# COMMAND ----------

# Step 2 : Generate secret : Generate a secret for the Service Principal you just created from the left hand side menu > Certifications & Secrets.
client_secret = "1aR8Q~ZB5HwOaRUY76qg9JDfXYdxMxnyZQI8jbr-"

# COMMAND ----------

# Step 3 : Setup spark Config

spark.conf.set("fs.azure.account.auth.type.formuladl101.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formuladl101.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formuladl101.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formuladl101.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formuladl101.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# Step 4 : Assign "Storage Blob Data Contributor" Role to Service Principal. So if Databricks can reach Service Principal, it already has full permission to Azure Data Lake Storage.

# Go to storage you will access via Databricks. 
# Click Access Control > Add > Storage Blob Data Contributor > Next > Select Members > Type name of the service principal you created > Create.

display(dbutils.fs.ls("abfss://demo@formuladl101.dfs.core.windows.net"))



# COMMAND ----------


