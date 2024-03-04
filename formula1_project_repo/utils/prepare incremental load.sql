-- Databricks notebook source
DROP DATABASE IF EXISTS formuladl101_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formuladl101_processed
LOCATION "/mnt/formuladl101/stage"

-- COMMAND ----------

DROP DATABASE IF EXISTS formuladl101_presentation CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formuladl101_presentation
LOCATION "/mnt/formuladl101/presentation"

-- COMMAND ----------


