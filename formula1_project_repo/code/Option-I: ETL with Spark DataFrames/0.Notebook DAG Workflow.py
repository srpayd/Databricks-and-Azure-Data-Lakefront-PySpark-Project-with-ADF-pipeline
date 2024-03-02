# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1. Ingest Files",0)


# COMMAND ----------

if v_result == 'Success':
    v2_result = dbutils.notebook.run("2. Presentation Files",0)
    if v2_result == 'Success':
        print('... Wokflow is completed succesfully ... ')
    else:
        print('... An error occurred while processing Presentation Files ... ')
else:
    print(' ... An error occurred while processing Presentation Files ... ')
