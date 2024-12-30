# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS hospitalrcm_adb.audit;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hospitalrcm_adb.audit.load_logs (
# MAGIC     data_source STRING,
# MAGIC     tablename STRING,
# MAGIC     numberofrowscopied INT,
# MAGIC     watermarkcolumnname STRING,
# MAGIC     loaddate TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM audit.load_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC TRUNCATE TABLE hospitalrcm_adb.audit.load_logs;

# COMMAND ----------


