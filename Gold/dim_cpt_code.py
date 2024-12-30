# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DELTA.`/mnt/gold/dim_cpt_code`
# MAGIC (
# MAGIC cpt_codes string,
# MAGIC procedure_code_category string,
# MAGIC procedure_code_descriptions string,
# MAGIC code_status string,
# MAGIC refreshed_at timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate TABLE DELTA.`/mnt/gold/dim_cpt_code` 

# COMMAND ----------

gold_cpt_code_df = spark.read.format('delta').load("/mnt/gold/dim_cpt_code")
gold_cpt_code_df.createOrReplaceTempView("gold_cpt_code")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta.`/mnt/gold/dim_cpt_code`
# MAGIC select 
# MAGIC cpt_codes,
# MAGIC procedure_code_category,
# MAGIC procedure_code_descriptions ,
# MAGIC code_status,
# MAGIC current_timestamp() as refreshed_at
# MAGIC  from DELTA.`/mnt/silver/cptcodes`
# MAGIC  where is_quarantined=false and is_current=true

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from delta.`/mnt/gold/dim_cpt_code` LIMIT 10
