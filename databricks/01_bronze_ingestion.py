#Databricks Notebook: 01_bronze_ingestion

# Objective: Load csv raw and store it in Delta Lake without modifications
## keeping a full historical record from the original data

# Enviroment settings
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
import kagglehub

spark = SparkSession.builder \
  .appName("RetailBronze") \
  .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
  .config("spark.sql.extensons","io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .getOrCreate()


#Define the schema to guarantee consistency

schema = StructType([
  StructField("Transaction ID", IntegerType(), True),
  StructField("Date", StringType(), True),
  StructField("Customer ID", IntegerType(), True),
  StructField("Gender", StringType(), True),
  StructField("Age", IntegerType(), True),
  StructField("Product Category", StringType(), True),
  StructField("Quantity", IntegerType(), True),
  StructField("Price per Unit", DoubleType(), True),
  StructField("Total Amount", DoubleType(), True)
])

# CSV file path 
file_path = kagglehub.dataset_download("mohammadtalib786/retail-sales-dataset")
#print("Path to dataset files:", file_path)

#CSV read with define schema
df_bronze = spark.read \
  .option("header", "true") \
  .option("inferSchema", "false") \
  .schema(schema) \
  .csv(file_path)

#Add metadata in audit
from pyspark.sql.functions import current_timestamp, input_file_name

df_bronze = df_bronze.withColumn("ingestion_timestamp", current_timestamp()) \
  .withColumn("source_file", input_file_name())

# Show data sample
df_bronze.show(10, truncate=False)

for col_name in df_bronze.columns:
    df_bronze = df_bronze.withColumnRenamed(col_name, col_name.replace(" ", "_"))

#Save bronze layer on Data Lake
df_bronze.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save("./data/datalake/bronze/retail_sales")


#Create table in the metastore

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.retail_sales 
    USING DELTA 
    LOCATION './data/datalake/bronze/retail_sales'
""")

print("✅ Datos cargados exitosamente en la capa Bronze")
