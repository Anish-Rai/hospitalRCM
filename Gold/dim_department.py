# Databricks notebook source
path = '/mnt/gold/dim_department'

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS DELTA.`{path}`
(
    Dept_Id string,
    SRC_Dept_Id string,
    Name string,
    datasource string
)
""")

# COMMAND ----------

spark.sql(f"""
  truncate TABLE DELTA.`{path}`        
""") 

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into DELTA.`/mnt/gold/dim_department`
# MAGIC select 
# MAGIC distinct
# MAGIC Dept_Id ,
# MAGIC SRC_Dept_Id ,
# MAGIC Name ,
# MAGIC datasource 
# MAGIC  from  DELTA.`/mnt/silver/departments`
# MAGIC  where is_quarantined=false

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`/mnt/gold/dim_department` LIMIT 10         

# COMMAND ----------


