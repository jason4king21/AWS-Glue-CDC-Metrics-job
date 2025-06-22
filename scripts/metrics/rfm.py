%idle_timeout 2880
%glue_version 5.0
%worker_type G.1X
%number_of_workers 5
%connections "BISqlserverConn"

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)





from pyspark.sql.functions import col, to_date
from datetime import datetime
from awsglue.context import GlueContext
from pyspark.context import SparkContext

spark_context = SparkContext.getOrCreate()
glueContext = GlueContext(spark_context)
spark = glueContext.spark_session

bucket = "jk-business-insights-assessment"
today_str = datetime.now().strftime("%Y-%m-%d")

# ---------- Control File ----------
def read_control_date(bucket, control_key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=control_key)
        return obj["Body"].read().decode("utf-8").strip()
    except:
        return "2020-01-01"

def update_control_date(bucket, control_key, max_date):
    s3.put_object(Bucket=bucket, Key=control_key, Body=max_date)

def process_silver_order_items():
    control_key = "control/silver_order_items_last_run.txt"
    last_run = read_control_date(bucket, control_key)

    df_raw = spark.read.parquet(f"s3://{bucket}/data/bronze/order_items/{today_str}/") \
        .withColumn("CREATION_DATE", to_date("CREATION_TIME_UTC")) \
        .filter(col("CREATION_DATE") > last_run)

    if df_raw.rdd.isEmpty():
        print("✅ No new order_items to process.")
        return

    df_clean = df_raw.withColumn("ITEM_PRICE", col("ITEM_PRICE").cast("double")) \
                     .dropDuplicates(["ORDER_ID", "LINEITEM_ID"])

    df_clean.repartition("CREATION_DATE").write.mode("append") \
        .option("compression", "snappy") \
        .partitionBy("CREATION_DATE") \
        .parquet(f"s3://{bucket}/data/silver/order_items/")

    max_date = df_clean.agg({"CREATION_DATE": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ silver_order_items processed through {max_date}")

def process_silver_order_item_options():
    control_key = "control/silver_order_item_options_last_run.txt"
    last_run = read_control_date(bucket, control_key)

    df_raw_opts = spark.read.parquet(f"s3://{bucket}/data/bronze/order_item_options/{today_str}/") \
        .withColumn("CREATION_DATE", to_date("cdc_timestamp")) \
        .filter(col("CREATION_DATE") > last_run)

    if df_raw_opts.rdd.isEmpty():
        print("✅ No new order_item_options to process.")
        return

    df_opts = df_raw_opts.withColumn("OPTION_PRICE", col("OPTION_PRICE").cast("double")) \
                         .dropDuplicates(["ORDER_ID", "LINEITEM_ID"])

    df_opts.repartition("CREATION_DATE").write.mode("append") \
        .option("compression", "snappy") \
        .partitionBy("CREATION_DATE") \
        .parquet(f"s3://{bucket}/data/silver/order_item_options/")

    max_date = df_opts.agg({"CREATION_DATE": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ silver_order_item_options processed through {max_date}")

def process_silver_order_revenue():
    df_items = spark.read.parquet(f"s3://{bucket}/data/silver/order_items/")
    df_options = spark.read.parquet(f"s3://{bucket}/data/silver/order_item_options/") \
        .drop("CREATION_DATE") \
        .drop("cdc_action") \
        .drop("cdc_timestamp") \
        .drop("ingestion_timestamp")

    df_revenue = df_items.join(df_options, ["ORDER_ID", "LINEITEM_ID"], "left") \
        .na.fill({"OPTION_PRICE": 0.0}) \
        .withColumn("TOTAL_REVENUE", col("ITEM_PRICE") + col("OPTION_PRICE"))


    df_revenue.repartition("CREATION_DATE").write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("CREATION_DATE") \
        .parquet(f"s3://{bucket}/data/silver/order_revenue/")

    print("✅ silver_order_revenue written successfully.")
    
def process_silver_date_dim():
    control_key = "control/silver_date_dim_last_run.txt"
    last_run = read_control_date(bucket, control_key)

    df_raw = spark.read.parquet(f"s3://{bucket}/data/bronze/date_dim/{today_str}/") \
        .withColumn("CREATION_DATE", to_date("date_key")) \
        .filter(col("CREATION_DATE") > last_run)

    if df_raw.rdd.isEmpty():
        print("✅ No new date_dim records to process.")
        return

    df_clean = df_raw.dropDuplicates(["date_key"])

    df_clean.repartition("CREATION_DATE").write.mode("append") \
        .option("compression", "snappy") \
        .partitionBy("CREATION_DATE") \
        .parquet(f"s3://{bucket}/data/silver/date_dim/")

    max_date = df_clean.agg({"CREATION_DATE": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ silver_date_dim processed through {max_date}")

    
    
process_silver_order_items()
process_silver_order_item_options()
process_silver_date_dim()
process_silver_order_revenue()





from pyspark.sql.functions import (
    col, sum as _sum, countDistinct, count, avg, to_date,
    year, weekofyear, countDistinct, dense_rank
)
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Setup
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

bucket = "jk-business-insights-assessment"
path_items = f"s3://{bucket}/data/silver/order_items/"
path_revenue = f"s3://{bucket}/data/silver/order_revenue/"
gold_path = f"s3://{bucket}/data/gold/mart_location_performance/"

# Load and join
df_items = spark.read.parquet(path_items).select("ORDER_ID", "RESTAURANT_ID", "CREATION_TIME_UTC")
df_revenue = spark.read.parquet(path_revenue).select("ORDER_ID", "TOTAL_REVENUE")

df = df_items.join(df_revenue, "ORDER_ID", "inner") \
    .withColumn("ORDER_DATE", to_date("CREATION_TIME_UTC")) \
    .withColumn("YEAR", year("ORDER_DATE")) \
    .withColumn("WEEK", weekofyear("ORDER_DATE"))

# Aggregate per location
df_metrics = df.groupBy("RESTAURANT_ID").agg(
    _sum("TOTAL_REVENUE").alias("TOTAL_REVENUE"),
    countDistinct("ORDER_ID").alias("NUM_ORDERS"),
    countDistinct("ORDER_DATE").alias("ACTIVE_DAYS"),
    countDistinct("WEEK").alias("ACTIVE_WEEKS")
).withColumn(
    "AVG_ORDER_VALUE", col("TOTAL_REVENUE") / col("NUM_ORDERS")
).withColumn(
    "ORDERS_PER_DAY", col("NUM_ORDERS") / col("ACTIVE_DAYS")
).withColumn(
    "ORDERS_PER_WEEK", col("NUM_ORDERS") / col("ACTIVE_WEEKS")
)

# Rank locations by total revenue
window_spec = Window.orderBy(col("TOTAL_REVENUE").desc())
df_ranked = df_metrics.withColumn("REVENUE_RANK", dense_rank().over(window_spec))

# Write to gold layer
df_ranked.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_path)

print("✅ mart_location_performance written successfully.")






from pyspark.sql.functions import (
    col, sum as _sum, countDistinct, avg, when
)
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Setup
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

bucket = "jk-business-insights-assessment"
path_items = f"s3://{bucket}/data/silver/order_items/"
path_options = f"s3://{bucket}/data/silver/order_item_options/"
path_revenue = f"s3://{bucket}/data/silver/order_revenue/"
gold_path = f"s3://{bucket}/data/gold/mart_discount_effectiveness/"

# Load data
df_items = spark.read.parquet(path_items).select("ORDER_ID", "LINEITEM_ID", "USER_ID")
df_options = spark.read.parquet(path_options).select("ORDER_ID", "LINEITEM_ID", "OPTION_PRICE")
df_revenue = spark.read.parquet(path_revenue).select("ORDER_ID", "TOTAL_REVENUE")

# Join data to associate discounts with orders
df_joined = df_items.join(df_options, ["ORDER_ID", "LINEITEM_ID"], "left") \
                    .join(df_revenue, "ORDER_ID", "left") \
                    .withColumn("IS_DISCOUNTED", when(col("OPTION_PRICE") < 0, 1).otherwise(0))

# Classify entire order as discounted if **any** line item has a discount
df_discount_flag = df_joined.groupBy("ORDER_ID").agg(
    _sum("IS_DISCOUNTED").alias("DISCOUNTED_LINES"),
    _sum("TOTAL_REVENUE").alias("ORDER_REVENUE")
).withColumn(
    "IS_DISCOUNTED_ORDER", when(col("DISCOUNTED_LINES") > 0, "Yes").otherwise("No")
)

# Aggregate summary by discount flag
df_summary = df_discount_flag.groupBy("IS_DISCOUNTED_ORDER").agg(
    countDistinct("ORDER_ID").alias("NUM_ORDERS"),
    _sum("ORDER_REVENUE").alias("TOTAL_REVENUE"),
    avg("ORDER_REVENUE").alias("AVG_ORDER_VALUE")
)

# Write to gold layer
df_summary.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_path)

print("Discounted item rows:", df_options.filter(col("OPTION_PRICE") < 0).count())

print("✅ mart_discount_effectiveness written successfully.")






from pyspark.sql.functions import col, max, countDistinct, sum as _sum, datediff, lit, to_date
from datetime import datetime, timedelta
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Setup
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

bucket = "jk-business-insights-assessment"
silver_path = f"s3://{bucket}/data/silver/order_revenue/"
gold_path = f"s3://{bucket}/data/gold/mart_customer_rfm/"
today = datetime.now()
today_str = today.strftime("%Y-%m-%d")
rfm_window_days = 90

# Load silver data
df = spark.read.parquet(silver_path)
df = df.withColumn("CREATION_DATE", to_date("CREATION_TIME_UTC"))

# Filter data for the RFM window
cutoff_date = (today - timedelta(days=rfm_window_days)).strftime("%Y-%m-%d")
# df_filtered = df.filter(col("CREATION_DATE") >= cutoff_date)
df_filtered = df

# Compute Recency, Frequency, Monetary
last_purchase = df.groupBy("USER_ID") \
    .agg(max("CREATION_DATE").alias("LAST_PURCHASE_DATE"))

rfm = df_filtered.groupBy("USER_ID") \
    .agg(
        countDistinct("ORDER_ID").alias("FREQUENCY"),
        _sum("TOTAL_REVENUE").alias("MONETARY")
    ) \
    .join(last_purchase, "USER_ID", "left") \
    .withColumn("RECENCY", datediff(lit(today_str), col("LAST_PURCHASE_DATE")))

# Segment Customers
rfm_segmented = rfm.withColumn(
    "SEGMENT",
    when((col("RECENCY") <= 15) & (col("FREQUENCY") >= 5) & (col("MONETARY") >= 100), "VIP")
    .when((col("FREQUENCY") <= 1) & (col("RECENCY") <= 15), "New")
    .when((col("RECENCY") > 45) & (col("FREQUENCY") <= 2), "Churn Risk")
    .otherwise("Standard")
)

# Write to gold layer
rfm_segmented.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_path)

print("✅ mart_customer_rfm written successfully.")






import s3fs
import pandas as pd

fs = s3fs.S3FileSystem(anon=False)
df = pd.read_parquet("s3://jk-business-insights-assessment/data/gold/mart_customer_rfm/part-00001-1708846b-d765-4a69-945b-123f817a7e67-c000.snappy.parquet", filesystem=fs)
print(df.head())


bucket = "jk-business-insights-assessment"
silver_path = f"s3://{bucket}/data/silver/order_revenue/"

df = spark.read.parquet(silver_path)
print(df.head())