# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DELTA.`/mnt/gold/dim_patient`(
# MAGIC     patient_key STRING,
# MAGIC     src_patientid STRING,
# MAGIC     firstname STRING,
# MAGIC     lastname STRING,
# MAGIC     middlename STRING,
# MAGIC     ssn STRING,
# MAGIC     phonenumber STRING,
# MAGIC     gender STRING,
# MAGIC     dob DATE,
# MAGIC     address STRING,
# MAGIC     datasource STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate TABLE DELTA.`/mnt/gold/dim_patient`

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into DELTA.`/mnt/gold/dim_patient`
# MAGIC select 
# MAGIC      patient_key ,
# MAGIC     src_patientid ,
# MAGIC     firstname ,
# MAGIC     lastname ,
# MAGIC     middlename ,
# MAGIC     ssn ,
# MAGIC     phonenumber ,
# MAGIC     gender ,
# MAGIC     dob ,
# MAGIC     address ,
# MAGIC     datasource 
# MAGIC  from DELTA.`/mnt/silver/patients`
# MAGIC  where is_current=true and is_quarantined=false

# COMMAND ----------

