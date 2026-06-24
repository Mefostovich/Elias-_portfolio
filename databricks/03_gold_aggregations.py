from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark Session with support for Delta Lake
spark = SparkSession.builder \
  .appName("GoldAggregations") \
  .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .getOrCreate()


# read from Silver layer
df_silver = spark.read.format("delta").load('./data/datalake/silver/retail_sales')
df_silver.show(10, truncate=False)

# --- Table 1: Daily sales per category ---
df_daily_sales = df_silver.groupBy("Date", "Product_Category") \
  .agg(
    sum("Total_Amount").alias("Daily_Revenue"),
    sum("Quantity").alias("Daily_Units_Sold"),
    count("Transaction_ID").alias("Daily_Transactions"),
    avg("Price_per_Unit").alias("Avg_Price")
  ) \
  .orderBy(asc("Date"))


df_daily_sales.write \
  .format("delta") \
  .mode("overwrite") \
  .save("./data/datalake/gold/daily_sales_by_category")

print("📊 Métricas diarias por Categoría de Producto:")
df_daily_sales.show(10, truncate=False)

# --- Table 2: Montly Sales per category ---
df_monthly_sales = df_silver.groupBy("Year", "Month", "Product_Category") \
  .agg(
    sum("Total_Amount").alias("Monthly_Revenue"),
    sum("Quantity").alias("Monthly_Units_Sold"),
    count("Transaction_ID").alias("Monthly_Transactions"),
    countDistinct("Customer_ID").alias("Unique_Customers")
  ) \
  .orderBy("Year","Month")

df_monthly_sales.write \
  .format("delta") \
  .mode("overwrite") \
  .save('./data/datalake/gold/monthly_sales_by_category')

print("📊 Métricas mensuales por Categoría de Producto:")
df_monthly_sales.show(10, truncate=False)

# --- 3: KPIs per client ---
df_customer_metrics = df_silver.groupBy("Customer_ID", "Gender", "AgeGroup") \
  .agg(
    sum("Total_Amount").alias("Total_Spent"),
    count("Transaction_ID").alias("Purchase_Count"),
    avg("Total_Amount").alias("Avg_Order_Value"),
    max("Date").alias("Last_Purchase_Date")
  ) \
  .orderBy(desc("Total_Spent"))

df_customer_metrics.write \
  .format("delta") \
  .mode("overwrite") \
  .save('./data/datalake/gold/customer_metrics')

print("📊 KPIs por Cliente:")
df_customer_metrics.show(10, truncate=False)

# --- Table 4: Daily KPIs summary
df_daily_kpis = df_silver.groupBy("Date") \
    .agg(
        sum("Total_Amount").alias("Total_Revenue"),
        sum("Quantity").alias("Total_Units"),
        count("Transaction_ID").alias("Total_Transactions"),
        countDistinct("Customer_ID").alias("Unique_Customers"),
        avg("Total_Amount").alias("Avg_Order_Value")
)

df_daily_kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .save("./data/datalake/gold/daily_kpis")

print("📊 KPIs diarios:")
df_daily_kpis.show(10, truncate=False)

spark.sql(f"CREATE DATABASE IF NOT EXISTS gold")

base_path = "./data/datalake/gold"

tables =["daily_sales_by_category", "monthly_sales_by_category", 
              "customer_metrics", "daily_kpis"]

# Register table in local SQL catalog
for table in tables:
    spark.sql(f"""
          CREATE TABLE IF NOT EXISTS gold.{table}
          USING DELTA
          LOCATION './data/datalake/gold/'
          """)

print("✅ Capa Gold procesada y registrada exitosamente.")