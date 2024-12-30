# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

claims_df=spark.read.csv("/mnt/landing/claims/*.csv",header=True)

claims_df = claims_df.withColumn(
    "datasource",
    f.when(f.input_file_name().contains("hospital1"), "hosa").when(f.input_file_name().contains("hospital2"), "hosb")
     .otherwise(None)
)

display(claims_df)

claims_df.write.format("parquet").mode("overwrite").save("/mnt/bronze/claims/")

claims_df.createOrReplaceTempView("claims")
