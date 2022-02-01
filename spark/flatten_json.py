import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("flatJson").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

json_df = spark.read.format("json")\
                    .option("multiline","true")\
                    .option("header","true")\
                    .option("inferSchema","true")\
                    .load("/FileStore/tables/practise/json/FBL3N_MULTIPLE_NESTED_JSON_WITH_SAME_COL_IN_DIFF_NEST.json")

flat_json_df = json_df.withColumn("document_type_inr", json_df["document_type.inr"])\
                      .withColumn("documnet_type_type", json_df["document_type.type"])\
                      .withColumn("Local_Currency_INR", json_df["Local_Currency.inr"])\
                      .withColumn("Local_Currency_UR", json_df["Local_Currency.ur"])\
                      .withColumn("Local_Currency_USD", json_df["Local_Currency.usd"]).drop("document_type", "Local_Currency")
df = flat_json_df.select("Amount_in_Local_Currency","Assignment","Business_Area","Cleared_Open_Items_Symbol","Clearing_Document","Document_Date","Document_Number","document_type_inr","documnet_type_type","Local_Currency_INR","Local_Currency_UR","Local_Currency_USD","Posting_Key","Tax_Code","Text")

df.show(truncate=False)
