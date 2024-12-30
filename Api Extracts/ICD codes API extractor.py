# Databricks notebook source
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType

# Initialize Spark session
spark = SparkSession.builder.appName("ICD_Code_Extraction").getOrCreate()

# Authentication details
client_id = 'ef47340d-d543-4d0a-9092-8caee773428f_538f106a-f81e-4310-acdc-277e015ed2d2'
client_secret = '01N701Ui0GggiLcVw94nnIrpGQIgq0NWOScMb9ZLhQI='
auth_url = 'https://icdaccessmanagement.who.int/connect/token'
base_url = 'https://id.who.int/icd/'  # Base URL for the ICD API

# Current date for timestamps
current_date = datetime.now().date()

# Authenticate and retrieve access token
auth_response = requests.post(auth_url, data={
    'client_id': client_id,
    'client_secret': client_secret,
    'grant_type': 'client_credentials'
})

if auth_response.status_code == 200:
    access_token = auth_response.json().get('access_token')
else:
    raise Exception(f"Failed to obtain access token: {auth_response.status_code} - {auth_response.text}")

# Headers for API requests
headers = {
    'Authorization': f'Bearer {access_token}',
    'API-Version': 'v2',  # Specify API version
    'Accept-Language': 'en',  # Language preference for response
}

# Function to fetch data from the API
def fetch_icd_data(url):
    """
    Fetch data from the WHO ICD API for a given URL.
    """
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

# Recursive function to extract ICD codes
def extract_icd_codes(url):
    """
    Recursively fetch and extract ICD codes and their descriptions from the WHO API.
    """
    data = fetch_icd_data(url)
    codes = []
    
    # If the response has child URLs, fetch data recursively
    if 'child' in data:
        for child_url in data['child']:
            codes.extend(extract_icd_codes(child_url))
    else:
        # Extract and structure the ICD code details
        if 'code' in data and 'title' in data:
            codes.append({
                'icd_code': data['code'],
                'icd_code_type': 'ICD-10',
                'code_description': data['title']['@value'],
                'inserted_date': current_date,
                'updated_date': current_date,
                'is_current_flag': True
            })
    return codes

# Root URL for ICD-10 codes (modify as per specific chapter or category)
root_url = 'https://id.who.int/icd/release/10/2019/A00-A09'

# Fetch all ICD codes starting from the root URL
icd_codes = extract_icd_codes(root_url)

# Define schema for the PySpark DataFrame
schema = StructType([
    StructField("icd_code", StringType(), True),
    StructField("icd_code_type", StringType(), True),
    StructField("code_description", StringType(), True),
    StructField("inserted_date", DateType(), True),
    StructField("updated_date", DateType(), True),
    StructField("is_current_flag", BooleanType(), True)
])

# Convert the data to a PySpark DataFrame
df = spark.createDataFrame(icd_codes, schema=schema)

# Save the DataFrame to the data lake in Parquet format
output_path = "/mnt/bronze/icd_codes/"
df.write.format("parquet").mode("append").save(output_path)

print(f"ICD codes successfully saved to {output_path}")

