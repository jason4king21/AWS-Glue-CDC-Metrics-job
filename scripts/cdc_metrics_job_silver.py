import s3fs
import pandas as pd
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, lit, to_date, when, lag, avg, max, countDistinct,
    sum as _sum, datediff, current_timestamp, row_number, date_format,
    year, month, weekofyear, hour, concat_ws, dense_rank
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import boto3
import time
import traceback



# --- Init
spark_context = SparkContext.getOrCreate()
glueContext = GlueContext(spark_context)
spark = glueContext.spark_session
s3 = boto3.client("s3")

bucket = "jk-business-insights-assessment"
today = datetime.now()
today_str = today.strftime("%Y-%m-%d")
connection_name = "BISqlserverConn"
control_key = "control/cdc/order_items/last_run.txt"

# ---------- Control File ----------
def get_last_run_time():
    try:
        obj = s3.get_object(Bucket=bucket, Key=control_key)
        return obj["Body"].read().decode("utf-8").strip()
    except:
        return "2020-01-01"

def update_last_run_time(new_time):
    s3.put_object(Bucket=bucket, Key=control_key, Body=new_time)


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

