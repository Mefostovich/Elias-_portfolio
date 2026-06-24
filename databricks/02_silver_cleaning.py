# Databricks Notebook: 02_solver_cleaning

# Silver layer - Cleaning and transformation
## Objective: Clean, standarize and entich the data
## Apply quality rules and delete invalid records

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# Start a Spark Session with support for Delta Lake
spark = SparkSession.builder \
    .appName("SilverCleaning") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read from Bronze layer
df_bronze = spark.read.format("delta").load('./data/datalake/bronze/retail_sales')


# --- 1. Data cleaning ---
# Turn Date to date type
df_silver = df_bronze.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

#Filter invalid records
df_silver = df_silver.filter(
  (col("Quantity") > 0) &
  (col("Price_per_Unit") > 0) &
  (col("Total_Amount") > 0) &
  (col("Age").between(0,120)) &
  (col("Gender").isin("Male", "Female"))
)

# --- 2. Standarize
# Clean product categories
df_silver = df_silver.withColumn(
  "Product_Category",
  when(col("Product_Category").rlike("(?i)clothing|apparel"), "Clothing")
  .when(col("Product_Category").rlike("(?i)electronic|gadger|tech"), "Electronics")
  .when(col("Product_Category").rlike("(?i)book|stationery"), "Books")
  .otherwise("Other")
)




# --- 3. Enrichment --- 
# Add derive columns
df_silver = df_silver.withColumn("Year", year(col("Date"))) \
  .withColumn("Month", month(col("Date"))) \
  .withColumn("Day", dayofmonth(col("Date"))) \
  .withColumn("DayOfWeek", dayofweek(col("Date"))) \
  .withColumn("Quarter", quarter(col("Date"))) \
  .withColumn("AgeGroup",
              when(col("Age") < 25, "18-24")
              .when(col("Age") < 35, "25-34")
              .when(col("Age") < 45, "35-44")
              .when(col("Age") < 55, "45-54")
              .otherwise("55+")
)
df_silver.show(10, truncate=False)

# --- 4. Data quality: register rejected records
# Identify records that didn't pass the filters
df_rejected = df_bronze.join(df_silver, ["Transaction_ID"], "left_anti")

# ¡CRUCIAL! Save CLEAN & TRANSFORMED recordsin the path Silver
df_silver.write \
  .format("delta") \
  .mode("overwrite") \
  .save('./data/datalake/silver/retail_sales')

# Save rejected records for auditing
df_rejected.write \
  .format("delta") \
  .mode("append") \
  .save('./data/datalake/silver/rejected_sales')

# Assure the db existance/local scheme
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# Register table linking to corrected path
spark.sql("""
          CREATE TABLE IF NOT EXISTS silver.retail_sales
          USING DELTA
          LOCATION './data/datalake/silver/retail_sales'
          """)

# Cleaning stats
total_original = df_bronze.count()
total_cleaned = df_silver.count()
total_rejected = df_rejected.count()

print(f"📊 Registros originales: {total_original}")
print(f"✅ Registros limpios: {total_cleaned}")
print(f"❌ Registros rechazados: {total_rejected}")
print(f"📈 Tasa de calidad: {(total_cleaned/total_original)*100:.2f}%")