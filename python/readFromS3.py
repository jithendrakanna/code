import sys
import os
os.environ['SPARK_HOME'] = "C:/spark/spark-3.0.3-bin-hadoop2.7"
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-11.0.14"
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'
sys.path.append("C:/spark/spark-3.0.3-bin-hadoop2.7/python")
sys.path.append("C:/spark/spark-3.0.3-bin-hadoop2.7/python/lib")

import boto3
import json

data = open("C:/Users/jithendra.kanna/Documents/AWS/s3_credentials.json")
s3_data = json.load(data)

s3 = boto3.client(
    service_name=s3_data['service_name'],
    region_name=s3_data['region_name'],
    aws_access_key_id=s3_data['aws_access_key_id'],
    aws_secret_access_key=s3_data['aws_secret_access_key']
)

bucket = "databricksdai-bkt"
path = "datalake/raw/employee/full/"
my_bucket = s3.list_objects(Bucket=bucket, Prefix=path)

file_list = []
for obj in my_bucket.get('Contents'):
    if "part-" in obj.get('Key'):
        file_list.append(obj.get('Key'))
    else:
        file_list.append(obj.get('Key'))

for i in file_list:
    if ".csv" in i:
        file = i
        file_format = file.split(".")[-1]
        break

print(file_list)
print(file)
print(file_format)

response = s3.get_object(
    Bucket=bucket,
    Key=str(file)
)

import pandas as pd
from io import BytesIO

csv_df = pd.read_csv(
    BytesIO(response['Body'].read())
)

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("readFromS3").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

df = spark.createDataFrame(csv_df)

df.printSchema()

df.show(truncate=False)
