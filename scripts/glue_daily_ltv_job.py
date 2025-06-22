


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

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, lit, to_date, when, lag, avg, max, countDistinct,
    sum as _sum, datediff
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

# ---------- Control Helpers ----------
def read_control_date(bucket, key, fallback="2020-01-01"):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8").strip()
    except:
        return fallback

def update_control_date(bucket, key, date_str):
    s3.put_object(Bucket=bucket, Key=key, Body=date_str)
    
def process_silver_order_items():
    print("🔹 Processing silver_order_items...")
    control_key = "control/silver_order_items_last_run.txt"
    last_run = read_control_date(bucket, control_key)

    # Load Bronze CDC
    df_raw = spark.read.parquet(f"s3://{bucket}/data/bronze/order_items/") \
                       .withColumn("CREATION_DATE", to_date("CREATION_TIME_UTC")) \
                       .filter(col("CREATION_DATE") > last_run)

    if df_raw.rdd.isEmpty():
        print("✅ No new order_items to process.")
        return

    df_clean = df_raw \
        .withColumn("ITEM_PRICE", col("ITEM_PRICE").cast("double")) \
        .dropDuplicates(["ORDER_ID", "LINEITEM_ID"])

    df_clean.repartition("CREATION_DATE").write.mode("append") \
        .option("compression", "snappy") \
        .partitionBy("CREATION_DATE") \
        .parquet(f"s3://{bucket}/data/silver/order_items/")

    max_date = df_clean.agg({"CREATION_DATE": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ silver_order_items processed through {max_date}")
    
def process_silver_order_item_options():
    print("🔹 Processing silver_order_item_options...")
    control_key = "control/silver_order_item_options_last_run.txt"
    last_run = read_control_date(bucket, control_key)

    df_raw_opts = spark.read.parquet(f"s3://{bucket}/data/bronze/order_item_options/") \
                             .withColumn("CREATION_DATE", to_date("CREATION_TIME_UTC")) \
                             .filter(col("CREATION_DATE") > last_run)

    if df_raw_opts.rdd.isEmpty():
        print("✅ No new order_item_options to process.")
        return

    df_opts = df_raw_opts \
        .withColumn("OPTION_PRICE", col("OPTION_PRICE").cast("double")) \
        .dropDuplicates(["ORDER_ID", "LINEITEM_ID"])

    df_opts.repartition("CREATION_DATE").write.mode("append") \
        .option("compression", "snappy") \
        .partitionBy("CREATION_DATE") \
        .parquet(f"s3://{bucket}/data/silver/order_item_options/")

    max_date = df_opts.agg({"CREATION_DATE": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ silver_order_item_options processed through {max_date}")

def process_silver_order_revenue():
    print("🔹 Processing silver_order_revenue...")
    df_items = spark.read.parquet(f"s3://{bucket}/data/silver/order_items/")
    df_options = spark.read.parquet(f"s3://{bucket}/data/silver/order_item_options/")

    df_revenue = df_items.join(
        df_options,
        on=["ORDER_ID", "LINEITEM_ID"],
        how="left"
    ).withColumn("OPTION_PRICE", col("OPTION_PRICE").na.fill(0.0)) \
     .withColumn("TOTAL_REVENUE", col("ITEM_PRICE") + col("OPTION_PRICE"))

    df_revenue.repartition("CREATION_DATE").write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("CREATION_DATE") \
        .parquet(f"s3://{bucket}/data/silver/order_revenue/")

    print("✅ silver_order_revenue written successfully.")


# ---------- Metric Functions ----------
def calculate_ltv(df_orders):
    control_key = "control/fact_ltv_last_run.txt"
    last_processed = read_control_date(bucket, control_key)

    df_orders = df_orders.withColumn("ITEM_PRICE", col("ITEM_PRICE").cast("double")) \
                         .withColumn("CREATION_DATE", to_date("CREATION_TIME_UTC")) \
                         .filter(col("CREATION_DATE") > last_processed)

    if df_orders.rdd.isEmpty():
        print("✅ No new LTV data to process.")
        return

    df_daily = df_orders.groupBy("USER_ID", "CREATION_TIME_UTC", "CREATION_DATE") \
                        .agg(_sum("ITEM_PRICE").alias("daily_revenue"))

    window_spec = Window.partitionBy("USER_ID").orderBy("CREATION_TIME_UTC") \
                        .rowsBetween(Window.unboundedPreceding, 0)

    df_ltv = df_daily.withColumn("cumulative_ltv", _sum("daily_revenue").over(window_spec))
    df_ltv = df_ltv.coalesce(10)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df_ltv.write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("CREATION_DATE") \
        .parquet(f"s3://{bucket}/data/gold/fact_ltv_daily/")

    max_date = df_ltv.agg({"CREATION_DATE": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ LTV processed through {max_date}")

def calculate_rfm(df_orders):
    control_key = "control/fact_rfm_last_run.txt"
    last_run_date = read_control_date(bucket, control_key)

    df_recent = df_orders.withColumn("order_date", to_date("CREATION_TIME_UTC")) \
                         .filter(col("order_date") > last_run_date)

    if df_recent.rdd.isEmpty():
        print("✅ No new RFM data to process.")
        return

    df_rfm = df_recent.groupBy("USER_ID").agg(
        _sum("ITEM_PRICE").alias("monetary_3mo"),
        countDistinct("ORDER_ID").alias("frequency_3mo"),
        max("order_date").alias("last_order_date")
    ).withColumn("recency_days", datediff(lit(today_str), col("last_order_date")))

    df_rfm = df_rfm.withColumn("segment", when((col("recency_days") <= 10) & (col("frequency_3mo") > 5), "VIP")
        .when((col("recency_days") > 45) & (col("frequency_3mo") <= 2), "Churn Risk")
        .when((col("recency_days") <= 15) & (col("frequency_3mo") <= 2), "New")
        .otherwise("Regular"))

    df_rfm.coalesce(1).write.mode("overwrite") \
          .option("compression", "snappy") \
          .parquet(f"s3://{bucket}/data/gold/mart_customer_rfm/")

    max_date = df_recent.agg({"order_date": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ RFM processed through {max_date}")

def calculate_churn_indicators(df_orders):
    control_key = "control/fact_churn_last_run.txt"
    last_run_date = read_control_date(bucket, control_key)

    df_filtered = df_orders.withColumn("CREATION_DATE", to_date("CREATION_TIME_UTC")) \
                           .filter(col("CREATION_DATE") > last_run_date)

    if df_filtered.rdd.isEmpty():
        print("✅ No new churn indicator data to process.")
        return

    window = Window.partitionBy("USER_ID").orderBy("CREATION_TIME_UTC")
    df = df_filtered.withColumn("prev_order", lag("CREATION_TIME_UTC").over(window))
    df = df.withColumn("gap_days", (col("CREATION_TIME_UTC").cast("long") - col("prev_order").cast("long")) / 86400)

    df = df.groupBy("USER_ID").agg(
        datediff(lit(today_str), max("CREATION_TIME_UTC")).alias("days_since_last"),
        avg("gap_days").alias("avg_order_gap_days"),
        _sum("ITEM_PRICE").alias("total_spend")
    ).withColumn("churn_flag", when(col("days_since_last") > 45, "at_risk").otherwise("active"))

    df.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket}/data/gold/mart_churn_indicators/")

    max_date = df_filtered.agg({"CREATION_DATE": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ Churn indicators processed through {max_date}")

def calculate_sales_trends(df_orders):
    control_key = "control/fact_sales_trends_last_run.txt"
    last_processed = read_control_date(bucket, control_key)

    df = df_orders.withColumn("order_date", to_date("CREATION_TIME_UTC")) \
                  .filter(col("order_date") > last_processed)

    if df.rdd.isEmpty():
        print("✅ No new sales trend data to process.")
        return

    df.groupBy("order_date").agg(_sum("ITEM_PRICE").alias("daily_sales")) \
        .coalesce(1).write.mode("append") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket}/data/gold/mart_sales_trends/")

    max_date = df.agg({"order_date": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ Sales trends updated through {max_date}")

def calculate_loyalty_impact(df_orders):
    df_orders.groupBy("USER_ID", "IS_LOYALTY").agg(
        _sum("ITEM_PRICE").alias("total_spend"),
        countDistinct("ORDER_ID").alias("order_count")
    ).coalesce(1).write.mode("overwrite") \
     .option("compression", "snappy") \
     .parquet(f"s3://{bucket}/data/gold/mart_loyalty_impact/")

def calculate_discount_effectiveness(df_orders, df_options):
    df_options = df_options.withColumn("OPTION_PRICE", col("OPTION_PRICE").cast("double"))
    df_orders = df_orders.withColumn("ITEM_PRICE", col("ITEM_PRICE").cast("double"))

    df_joined = df_orders.join(
        df_options,
        on=["ORDER_ID", "LINEITEM_ID"],
        how="left"
    ).withColumn("is_discounted", col("OPTION_PRICE") < 0)

    df_result = df_joined.groupBy("is_discounted").agg(
        _sum("ITEM_PRICE").alias("revenue"),
        countDistinct("ORDER_ID").alias("order_count")
    )

    df_result.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket}/data/gold/mart_discount_effectiveness/")

    print("✅ Discount effectiveness calculation complete.")

# ---------- Unified Runner ----------
def metrics_runner(df_orders, df_options):
    metrics = {
        "Customer LTV": lambda: calculate_ltv(df_orders),
        "RFM Segmentation": lambda: calculate_rfm(df_orders),
        "Churn Indicators": lambda: calculate_churn_indicators(df_orders),
        "Sales Trends": lambda: calculate_sales_trends(df_orders),
        "Loyalty Impact": lambda: calculate_loyalty_impact(df_orders),
        "Discount Effectiveness": lambda: calculate_discount_effectiveness(df_orders, df_options)
    }

    print("\n🔁 Starting metrics pipeline...\n")

    for name, func in metrics.items():
        print(f"➡️ Running: {name}")
        start_time = time.time()
        try:
            func()
            elapsed = round(time.time() - start_time, 2)
            print(f"✅ {name} completed in {elapsed} sec\n")
        except Exception as e:
            print(f"❌ {name} failed: {str(e)}")
            traceback.print_exc()
            continue

    print("🏁 All metric tasks attempted.\n")

# ---------- ETL Loader ----------
def load_and_run():
    print("\n🔁 Starting Silver Layer Processing with CDC...\n")
    process_silver_order_items()
    process_silver_order_item_options()
    process_silver_order_revenue()

    print("\n✅ Silver Layer complete. Loading clean data for metrics...\n")

    df_orders = spark.read.parquet(f"s3://{bucket}/data/silver/order_revenue/")
    df_options = spark.read.parquet(f"s3://{bucket}/data/silver/order_item_options/")

    metrics_runner(df_orders, df_options)


# Run main loader
load_and_run()


from pyspark.sql.functions import col, to_date
from datetime import datetime
from awsglue.context import GlueContext
from pyspark.context import SparkContext

spark_context = SparkContext.getOrCreate()
glueContext = GlueContext(spark_context)
spark = glueContext.spark_session

bucket = "jk-business-insights-assessment"
today_str = datetime.now().strftime("%Y-%m-%d")

def process_silver_order_items():
    control_key = "control/silver_order_items_last_run.txt"
    last_run = read_control_date(bucket, control_key)

    df_raw = spark.read.parquet(f"s3://{bucket}/data/bronze/order_items/") \
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

    df_raw_opts = spark.read.parquet(f"s3://{bucket}/data/bronze/order_item_options/") \
        .withColumn("CREATION_DATE", to_date("CREATION_TIME_UTC")) \
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
    df_options = spark.read.parquet(f"s3://{bucket}/data/silver/order_item_options/")

    df_revenue = df_items.join(df_options, ["ORDER_ID", "LINEITEM_ID"], "left") \
        .withColumn("OPTION_PRICE", col("OPTION_PRICE").na.fill(0.0)) \
        .withColumn("TOTAL_REVENUE", col("ITEM_PRICE") + col("OPTION_PRICE"))

    df_revenue.repartition("CREATION_DATE").write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("CREATION_DATE") \
        .parquet(f"s3://{bucket}/data/silver/order_revenue/")

    print("✅ silver_order_revenue written successfully.")
    
from pyspark.sql.functions import col, lit, to_date, when, lag, avg, max, countDistinct, sum as _sum, datediff
from pyspark.sql.window import Window

def calculate_ltv(df_orders):
    control_key = "control/fact_ltv_last_run.txt"
    last_processed = read_control_date(bucket, control_key)

    df_orders = df_orders.filter(col("CREATION_DATE") > last_processed)

    if df_orders.rdd.isEmpty():
        print("✅ No new LTV data to process.")
        return

    df_daily = df_orders.groupBy("USER_ID", "CREATION_DATE", "CREATION_TIME_UTC") \
                        .agg(_sum("TOTAL_REVENUE").alias("daily_revenue"))

    window_spec = Window.partitionBy("USER_ID").orderBy("CREATION_TIME_UTC") \
                        .rowsBetween(Window.unboundedPreceding, 0)

    df_ltv = df_daily.withColumn("cumulative_ltv", _sum("daily_revenue").over(window_spec))

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df_ltv.repartition("CREATION_DATE").write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("CREATION_DATE") \
        .parquet(f"s3://{bucket}/data/gold/fact_ltv_daily/")

    max_date = df_ltv.agg({"CREATION_DATE": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ LTV processed through {max_date}")

def calculate_rfm(df_orders):
    control_key = "control/fact_rfm_last_run.txt"
    last_run_date = read_control_date(bucket, control_key)

    df_recent = df_orders.filter(col("CREATION_DATE") > last_run_date)

    if df_recent.rdd.isEmpty():
        print("✅ No new RFM data to process.")
        return

    df_rfm = df_recent.groupBy("USER_ID").agg(
        _sum("TOTAL_REVENUE").alias("monetary_3mo"),
        countDistinct("ORDER_ID").alias("frequency_3mo"),
        max("CREATION_DATE").alias("last_order_date")
    ).withColumn("recency_days", datediff(lit(today_str), col("last_order_date")))

    df_rfm = df_rfm.withColumn("segment", when((col("recency_days") <= 10) & (col("frequency_3mo") > 5), "VIP")
        .when((col("recency_days") > 45) & (col("frequency_3mo") <= 2), "Churn Risk")
        .when((col("recency_days") <= 15) & (col("frequency_3mo") <= 2), "New")
        .otherwise("Regular"))

    df_rfm.coalesce(1).write.mode("overwrite") \
          .option("compression", "snappy") \
          .parquet(f"s3://{bucket}/data/gold/mart_customer_rfm/")

    max_date = df_recent.agg({"CREATION_DATE": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ RFM processed through {max_date}")

def calculate_churn_indicators(df_orders):
    control_key = "control/fact_churn_last_run.txt"
    last_run_date = read_control_date(bucket, control_key)

    df_filtered = df_orders.filter(col("CREATION_DATE") > last_run_date)

    if df_filtered.rdd.isEmpty():
        print("✅ No new churn indicator data to process.")
        return

    window = Window.partitionBy("USER_ID").orderBy("CREATION_TIME_UTC")
    df = df_filtered.withColumn("prev_order", lag("CREATION_TIME_UTC").over(window)) \
                    .withColumn("gap_days", (col("CREATION_TIME_UTC").cast("long") - col("prev_order").cast("long")) / 86400)

    df = df.groupBy("USER_ID").agg(
        datediff(lit(today_str), max("CREATION_TIME_UTC")).alias("days_since_last"),
        avg("gap_days").alias("avg_order_gap_days"),
        _sum("TOTAL_REVENUE").alias("total_spend")
    ).withColumn("churn_flag", when(col("days_since_last") > 45, "at_risk").otherwise("active"))

    df.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket}/data/gold/mart_churn_indicators/")

    max_date = df_filtered.agg({"CREATION_DATE": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ Churn indicators processed through {max_date}")

def calculate_sales_trends(df_orders):
    control_key = "control/fact_sales_trends_last_run.txt"
    last_processed = read_control_date(bucket, control_key)

    df = df_orders.filter(col("CREATION_DATE") > last_processed)

    if df.rdd.isEmpty():
        print("✅ No new sales trend data to process.")
        return

    df.groupBy("CREATION_DATE").agg(_sum("TOTAL_REVENUE").alias("daily_sales")) \
        .coalesce(1).write.mode("append") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket}/data/gold/mart_sales_trends/")

    max_date = df.agg({"CREATION_DATE": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    update_control_date(bucket, control_key, max_date)
    print(f"✅ Sales trends updated through {max_date}")

def calculate_loyalty_impact(df_orders):
    df_orders.groupBy("USER_ID", "IS_LOYALTY").agg(
        _sum("TOTAL_REVENUE").alias("total_spend"),
        countDistinct("ORDER_ID").alias("order_count")
    ).coalesce(1).write.mode("overwrite") \
     .option("compression", "snappy") \
     .parquet(f"s3://{bucket}/data/gold/mart_loyalty_impact/")

def calculate_discount_effectiveness(df_orders, df_options):
    df_joined = df_orders.join(
        df_options,
        on=["ORDER_ID", "LINEITEM_ID"],
        how="left"
    ).withColumn("is_discounted", col("OPTION_PRICE") < 0)

    df_result = df_joined.groupBy("is_discounted").agg(
        _sum("TOTAL_REVENUE").alias("revenue"),
        countDistinct("ORDER_ID").alias("order_count")
    )

    df_result.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket}/data/gold/mart_discount_effectiveness/")

    print("✅ Discount effectiveness calculation complete.")
    
# ---------- Control File ----------
def get_last_run_time():
    try:
        obj = s3.get_object(Bucket=bucket, Key=control_key)
        return obj["Body"].read().decode("utf-8").strip()
    except:
        return "2020-01-01"

def update_last_run_time(new_time):
    s3.put_object(Bucket=bucket, Key=control_key, Body=new_time)

# ---------- Metric Functions ----------
def calculate_ltv(df_orders):
    df_daily = df_orders.groupBy("USER_ID", "CREATION_TIME_UTC").agg(_sum("ITEM_PRICE").alias("daily_revenue"))
    window_spec = Window.partitionBy("USER_ID").orderBy("CREATION_TIME_UTC")
    df_ltv = df_daily.withColumn("cumulative_ltv", _sum("daily_revenue").over(window_spec))
    df_ltv.write.mode("append").partitionBy("USER_ID").parquet(f"s3://{bucket}/data/gold/fact_ltv_daily/")

def calculate_rfm(df_orders):
    df_recent = df_orders.withColumn("order_date", to_date("CREATION_TIME_UTC"))
    window_start = (today - timedelta(days=90)).strftime("%Y-%m-%d")
    df_recent = df_recent.filter(col("order_date") >= lit(window_start))
    df_rfm = df_recent.groupBy("USER_ID").agg(
        _sum("ITEM_PRICE").alias("monetary_3mo"),
        _sum(lit(1)).alias("frequency_3mo"),
        max("order_date").alias("last_order_date")
    )
    df_rfm = df_rfm.withColumn("recency_days", (lit(today_str).cast("date") - col("last_order_date")).cast("int"))
    df_rfm = df_rfm.withColumn("segment", when((col("recency_days") <= 10) & (col("frequency_3mo") > 5), "VIP")
        .when((col("recency_days") > 45) & (col("frequency_3mo") <= 2), "Churn Risk")
        .when((col("recency_days") <= 15) & (col("frequency_3mo") <= 2), "New")
        .otherwise("Regular"))
    df_rfm.write.mode("overwrite").parquet(f"s3://{bucket}/data/gold/mart_customer_rfm/")

def calculate_churn_indicators(df_orders):
    window = Window.partitionBy("USER_ID").orderBy("CREATION_TIME_UTC")
    df = df_orders.withColumn("prev_order", lag("CREATION_TIME_UTC").over(window))
    df = df.withColumn("gap_days", (col("CREATION_TIME_UTC").cast("long") - col("prev_order").cast("long")) / 86400)
    df = df.groupBy("USER_ID").agg(
        datediff(lit(today_str), max("CREATION_TIME_UTC")).alias("days_since_last"),
        avg("gap_days").alias("avg_order_gap_days"),
        _sum("ITEM_PRICE").alias("total_spend")
    ).withColumn("churn_flag", when(col("days_since_last") > 45, "at_risk").otherwise("active"))
    df.write.mode("overwrite").parquet(f"s3://{bucket}/data/gold/mart_churn_indicators/")

def calculate_sales_trends(df_orders):
    df = df_orders.withColumn("order_date", to_date("CREATION_TIME_UTC"))
    df.groupBy("order_date").agg(_sum("ITEM_PRICE").alias("daily_sales")) \
        .write.mode("append").parquet(f"s3://{bucket}/data/gold/mart_sales_trends/")

def calculate_loyalty_impact(df_orders):
    df_orders.groupBy("USER_ID", "IS_LOYALTY_MEMBER").agg(
        _sum("ITEM_PRICE").alias("total_spend"),
        _sum(lit(1)).alias("order_count")
    ).write.mode("overwrite").parquet(f"s3://{bucket}/data/gold/mart_loyalty_impact/")

def calculate_discount_effectiveness(df_orders):
    df = df_orders.withColumn("is_discounted", col("OPTION_PRICE") < 0)
    df.groupBy("is_discounted").agg(
        _sum("ITEM_PRICE").alias("revenue"),
        _sum(lit(1)).alias("order_count")
    ).write.mode("overwrite").parquet(f"s3://{bucket}/data/gold/mart_discount_effectiveness/")

# ---------- ETL Loop ----------
tables = [
    {"name": "order_items", "primary_keys": ["ORDER_ID", "LINEITEM_ID"], "calculate_ltv": True},
    {"name": "order_item_options", "primary_keys": ["ORDER_ID", "LINEITEM_ID", "OPTION_NAME"], "calculate_ltv": False},
    {"name": "date_dim", "primary_keys": ["date_key"], "calculate_ltv": False}
]

for table in tables:
    table_name = table["name"]
    primary_keys = table["primary_keys"]
    calculate_ltv_flag = table["calculate_ltv"]

    raw_path = f"s3://{bucket}/data/bronze/{table_name}/{today_str}/"
    cdc_path = f"s3://{bucket}/data/cdc/{table_name}/date={today_str}/"
    snapshot_path = f"s3://{bucket}/data/snapshots/{table_name}/latest/"

    if table_name == "order_items":
        last_run_time = get_last_run_time()
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="sqlserver",
            connection_options={
                "connectionName": connection_name,
                "dbtable": "order_items",
                "customSql": f"SELECT * FROM order_items WHERE CREATION_TIME_UTC >= '{last_run_time}'",
                "useConnectionProperties": True
            }
        )
    else:
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="sqlserver",
            connection_options={
                "connectionName": connection_name,
                "dbtable": table_name,
                "useConnectionProperties": True
            }
        )

    df_current = dynamic_frame.toDF().dropDuplicates()
    df_current.write.mode("overwrite").parquet(raw_path)

    if table_name == "order_items":
        df_cdc = df_current.withColumn("cdc_action", lit("insert"))
        df_cdc.write.mode("append").partitionBy("cdc_action").parquet(cdc_path)
        update_last_run_time(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    else:
        try:
            df_previous = spark.read.parquet(snapshot_path)
        except:
            df_previous = spark.createDataFrame([], df_current.schema)

        non_pk_cols = [c for c in df_current.columns if c not in primary_keys]
        df_inserts = df_current.subtract(df_previous).withColumn("cdc_action", lit("insert"))
        df_deletes = df_previous.subtract(df_current).withColumn("cdc_action", lit("delete"))
        join_expr = [df_current[k] == df_previous[k] for k in primary_keys]
        df_joined = df_current.alias("curr").join(df_previous.alias("prev"), join_expr, "inner")
        df_updates = df_joined.filter(" OR ".join([f"curr.{c} <> prev.{c}" for c in non_pk_cols])) \
            .select("curr.*").withColumn("cdc_action", lit("update"))
        df_cdc = df_inserts.union(df_updates).union(df_deletes)
        df_cdc.write.mode("overwrite").partitionBy("cdc_action").parquet(cdc_path)
        df_current.write.mode("overwrite").parquet(snapshot_path)

    # ---- Metrics for order_items
    if calculate_ltv_flag:
        df_valid_orders = df_cdc.filter(col("cdc_action") != "delete")

        calculate_ltv(df_valid_orders)
        calculate_rfm(df_valid_orders)
        calculate_churn_indicators(df_valid_orders)
        calculate_sales_trends(df_valid_orders)
        calculate_loyalty_impact(df_valid_orders)
        calculate_discount_effectiveness(df_valid_orders)

print("✅ Full CDC + Metrics pipeline complete for", today_str)


from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, lit, to_date, when, lag, avg, max, countDistinct,
    sum as _sum, datediff, current_timestamp
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

# ---------- ETL Loop ----------
tables = [
    {"name": "order_items", "primary_keys": ["ORDER_ID", "LINEITEM_ID"], "calculate_ltv": True},
    {"name": "order_item_options", "primary_keys": ["ORDER_ID", "LINEITEM_ID", "OPTION_NAME"], "calculate_ltv": False},
    {"name": "date_dim", "primary_keys": ["date_key"], "calculate_ltv": False}
]

for table in tables:
    table_name = table["name"]
    primary_keys = table["primary_keys"]
    calculate_ltv_flag = table["calculate_ltv"]

    raw_path = f"s3://{bucket}/data/bronze/{table_name}/{today_str}/"
    cdc_path = f"s3://{bucket}/data/cdc/{table_name}/date={today_str}/"
    snapshot_path = f"s3://{bucket}/data/snapshots/{table_name}/latest/"

    if table_name == "order_items":
        last_run_time = get_last_run_time()
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="sqlserver",
            connection_options={
                "connectionName": connection_name,
                "dbtable": "order_items",
                "customSql": f"SELECT * FROM order_items WHERE CREATION_TIME_UTC >= '{last_run_time}'",
                "useConnectionProperties": True
            }
        )
    else:
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="sqlserver",
            connection_options={
                "connectionName": connection_name,
                "dbtable": table_name,
                "useConnectionProperties": True
            }
        )

    df_current = dynamic_frame.toDF().dropDuplicates() \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("cdc_action", lit("insert")) \
        .withColumn("cdc_timestamp", current_timestamp())
    # df_current = df_current.withColumn("is_weekend", col("is_weekend").cast("int")) \
    #                    .withColumn("is_holiday", col("is_holiday").cast("int")) 
    df_current.write.mode("overwrite").parquet(raw_path)

    if table_name == "order_items":
        df_cdc = df_current.withColumn("cdc_action", lit("insert")) \
             .withColumn("cdc_timestamp", current_timestamp())
        df_cdc.write.mode("append").partitionBy("cdc_action").parquet(cdc_path)
        update_last_run_time(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    else:
        try:
            df_previous = spark.read.parquet(snapshot_path)
        except:
            df_previous = spark.createDataFrame([], df_current.schema)

        non_pk_cols = [c for c in df_current.columns if c not in primary_keys]
        df_inserts = df_current.subtract(df_previous) \
            .withColumn("cdc_action", lit("insert")) \
            .withColumn("cdc_timestamp", current_timestamp())
        df_deletes = df_previous.subtract(df_current) \
            .withColumn("cdc_action", lit("delete")) \
            .withColumn("cdc_timestamp", current_timestamp())
        join_expr = [df_current[k] == df_previous[k] for k in primary_keys]
        df_joined = df_current.alias("curr").join(df_previous.alias("prev"), join_expr, "inner")
        df_updates = df_joined.filter(" OR ".join([f"curr.{c} <> prev.{c}" for c in non_pk_cols])) \
            .select("curr.*") \
            .withColumn("cdc_action", lit("update")) \
            .withColumn("cdc_timestamp", current_timestamp())
        df_cdc = df_inserts.union(df_updates).union(df_deletes)
        df_cdc.write.mode("overwrite").partitionBy("cdc_action").parquet(cdc_path)
        df_current.write.mode("overwrite").parquet(snapshot_path)

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

from pyspark.sql.functions import to_date, sum as _sum
from pyspark.sql.window import Window

bucket = "jk-business-insights-assessment"
today_str = datetime.now().strftime("%Y-%m-%d")

df = spark.read.parquet(f"s3://{bucket}/data/silver/order_revenue/")
df = df.withColumn("CREATION_DATE", to_date("CREATION_TIME_UTC"))

df_daily = df.groupBy("USER_ID", "CREATION_DATE") \
             .agg(_sum("TOTAL_REVENUE").alias("DAILY_REVENUE"))

window_spec = Window.partitionBy("USER_ID").orderBy("CREATION_DATE") \
                    .rowsBetween(Window.unboundedPreceding, 0)

df_ltv = df_daily.withColumn("CUMULATIVE_LTV", _sum("DAILY_REVENUE").over(window_spec))

df_ltv.write.mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("CREATION_DATE") \
    .parquet(f"s3://{bucket}/data/gold/fact_ltv_daily/")


from pyspark.sql.functions import max as _max
from pyspark.sql.functions import row_number
import pandas as pd


bucket = "jk-business-insights-assessment"
today_str = datetime.now().strftime("%Y-%m-%d")

# Read fact_ltv_daily
df = spark.read.parquet(f"s3://{bucket}/data/gold/fact_ltv_daily/")

# Get latest LTV per USER_ID
window_spec = Window.partitionBy("USER_ID").orderBy(col("CREATION_DATE").desc())

df_latest = df.withColumn("rank", row_number().over(window_spec)) \
              .filter(col("rank") == 1) \
              .drop("rank")

df_latest.write.mode("overwrite") \
    .parquet(f"s3://{bucket}/data/gold/mart_customer_ltv_snapshot/")

df_pd = df_latest.select("USER_ID", "CUMULATIVE_LTV").toPandas()

df_pd["CLV_GROUP"] = pd.qcut(
    df_pd["CUMULATIVE_LTV"],
    q=[0, 0.2, 0.8, 1.0],
    labels=["Low", "Medium", "High"]
)

df_out = spark.createDataFrame(df_pd)

df_out.write.mode("overwrite").parquet(f"s3://{bucket}/data/gold/mart_customer_clv_segment/")



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

from pyspark.sql.functions import col, max, lag, avg, sum as _sum, datediff, to_date, lit, when
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Setup
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

bucket = "jk-business-insights-assessment"
silver_path = f"s3://{bucket}/data/silver/order_revenue/"
gold_path = f"s3://{bucket}/data/gold/mart_customer_churn_profile/"
today = datetime.now()
today_str = today.strftime("%Y-%m-%d")

# Load silver data
df = spark.read.parquet(silver_path)
df = df.withColumn("CREATION_DATE", to_date("CREATION_TIME_UTC"))

# Calculate Days Since Last Order
last_order = df.groupBy("USER_ID").agg(
    max("CREATION_DATE").alias("LAST_ORDER_DATE")
).withColumn("DAYS_SINCE_LAST_ORDER", datediff(lit(today_str), col("LAST_ORDER_DATE")))

# Calculate Average Gap Between Orders
window_spec = Window.partitionBy("USER_ID").orderBy("CREATION_DATE")
df_with_lag = df.withColumn("PREV_ORDER_DATE", lag("CREATION_DATE").over(window_spec)) \
                .withColumn("ORDER_GAP", datediff(col("CREATION_DATE"), col("PREV_ORDER_DATE")))

avg_gap = df_with_lag.groupBy("USER_ID").agg(avg("ORDER_GAP").alias("AVG_ORDER_GAP_DAYS"))

# Calculate % Change in Spend Over Last Two 30-Day Periods
cutoff_30 = (today - timedelta(days=30)).strftime("%Y-%m-%d")
cutoff_60 = (today - timedelta(days=60)).strftime("%Y-%m-%d")

df_last_30 = df.filter((col("CREATION_DATE") > cutoff_30))
df_prev_30 = df.filter((col("CREATION_DATE") > cutoff_60) & (col("CREATION_DATE") <= cutoff_30))

spend_last_30 = df_last_30.groupBy("USER_ID").agg(_sum("TOTAL_REVENUE").alias("SPEND_LAST_30"))
spend_prev_30 = df_prev_30.groupBy("USER_ID").agg(_sum("TOTAL_REVENUE").alias("SPEND_PREV_30"))

spend_compare = spend_last_30.join(spend_prev_30, "USER_ID", "outer") \
    .fillna(0, ["SPEND_LAST_30", "SPEND_PREV_30"]) \
    .withColumn("PCT_SPEND_CHANGE", when(col("SPEND_PREV_30") == 0, None)
                .otherwise((col("SPEND_LAST_30") - col("SPEND_PREV_30")) / col("SPEND_PREV_30") * 100))

# Merge all churn indicators
df_churn = last_order.join(avg_gap, "USER_ID", "outer") \
                     .join(spend_compare, "USER_ID", "outer")

# Add Churn Risk Tag
df_churn = df_churn.withColumn(
    "CHURN_RISK_TAG",
    when(col("DAYS_SINCE_LAST_ORDER") > 45, "At Risk")
    .when(col("DAYS_SINCE_LAST_ORDER") > 30, "Monitor")
    .otherwise("Active")
)

# Save to Gold Layer
df_churn.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_path)

print("✅ mart_customer_churn_profile written successfully.")


from pyspark.sql.functions import (
    col, to_date, date_format, sum as _sum, year, month, weekofyear, hour, concat_ws
)
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from datetime import datetime

# Setup
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

bucket = "jk-business-insights-assessment"
today_str = datetime.now().strftime("%Y-%m-%d")

# Load data
df_revenue = spark.read.parquet(f"s3://{bucket}/data/silver/order_revenue/").drop("CREATION_TIME_UTC").drop("RESTAURANT_ID").drop("ITEM_CATEGORY")
df_items = spark.read.parquet(f"s3://{bucket}/data/silver/order_items/")

# Join revenue and items for dimensional breakdowns
df = df_revenue.join(
    df_items.select("ORDER_ID", "LINEITEM_ID", "RESTAURANT_ID", "APP_NAME", "ITEM_CATEGORY", "CREATION_TIME_UTC"),
    on=["ORDER_ID", "LINEITEM_ID"],
    how="left"
)

# Add date/time columns
df = df.withColumn("CREATION_DATE", to_date("CREATION_TIME_UTC")) \
       .withColumn("YEAR", year("CREATION_DATE")) \
       .withColumn("MONTH", month("CREATION_DATE")) \
       .withColumn("YEAR_MONTH", concat_ws("-", col("YEAR"), col("MONTH"))) \
       .withColumn("YEAR", year("CREATION_DATE")) \
       .withColumn("WEEK", weekofyear("CREATION_DATE")) \
       .withColumn("YEAR_WEEK", concat_ws("-", col("YEAR"), col("WEEK"))) \
       .withColumn("HOUR_OF_DAY", hour("CREATION_TIME_UTC"))

# Aggregate daily
daily = df.groupBy("CREATION_DATE", "RESTAURANT_ID", "ITEM_CATEGORY") \
          .agg(_sum("TOTAL_REVENUE").alias("DAILY_REVENUE"))

# Aggregate weekly
weekly = df.groupBy("YEAR_WEEK", "RESTAURANT_ID", "ITEM_CATEGORY") \
           .agg(_sum("TOTAL_REVENUE").alias("WEEKLY_REVENUE"))

# Aggregate monthly
monthly = df.groupBy("YEAR_MONTH", "RESTAURANT_ID", "ITEM_CATEGORY") \
            .agg(_sum("TOTAL_REVENUE").alias("MONTHLY_REVENUE"))

# Optional: Revenue by hour of day (for time-of-day insights)
hourly = df.groupBy("HOUR_OF_DAY", "RESTAURANT_ID", "ITEM_CATEGORY") \
           .agg(_sum("TOTAL_REVENUE").alias("HOURLY_REVENUE"))

# Save each metric to its own path in gold layer
daily.write.mode("overwrite").option("compression", "snappy") \
    .parquet(f"s3://{bucket}/data/gold/mart_sales_trends/daily/")

weekly.write.mode("overwrite").option("compression", "snappy") \
    .parquet(f"s3://{bucket}/data/gold/mart_sales_trends/weekly/")

monthly.write.mode("overwrite").option("compression", "snappy") \
    .parquet(f"s3://{bucket}/data/gold/mart_sales_trends/monthly/")

hourly.write.mode("overwrite").option("compression", "snappy") \
    .parquet(f"s3://{bucket}/data/gold/mart_sales_trends/hourly/")

print("✅ mart_sales_trends written successfully (daily, weekly, monthly, hourly).")


from pyspark.sql.functions import (
    col, sum as _sum, countDistinct, count, when, avg
)
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Setup
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

bucket = "jk-business-insights-assessment"
path_items = f"s3://{bucket}/data/silver/order_items/"
path_revenue = f"s3://{bucket}/data/silver/order_revenue/"
gold_path = f"s3://{bucket}/data/gold/mart_loyalty_program_impact/"

# Load data
df_items = spark.read.parquet(path_items).select("ORDER_ID", "USER_ID", "IS_LOYALTY")
df_revenue = spark.read.parquet(path_revenue).select("ORDER_ID", "LINEITEM_ID", "TOTAL_REVENUE")

# Join revenue with loyalty flag
df_joined = df_revenue.join(df_items.dropDuplicates(["ORDER_ID"]), on=["ORDER_ID"], how="left")

# Compute per-customer LTV
df_ltv = df_joined.groupBy("USER_ID", "IS_LOYALTY") \
    .agg(_sum("TOTAL_REVENUE").alias("LIFETIME_VALUE"))

# Flag repeat customers
df_orders = df_items.groupBy("USER_ID", "IS_LOYALTY") \
    .agg(countDistinct("ORDER_ID").alias("NUM_ORDERS")) \
    .withColumn("IS_REPEAT", when(col("NUM_ORDERS") > 1, 1).otherwise(0))

# Merge LTV with order counts
df_combined = df_ltv.join(df_orders, ["USER_ID", "IS_LOYALTY"], "inner")

# Aggregate by loyalty flag
df_summary = df_combined.groupBy("IS_LOYALTY").agg(
    countDistinct("USER_ID").alias("NUM_CUSTOMERS"),
    avg("LIFETIME_VALUE").alias("AVG_SPEND_PER_CUSTOMER"),
    _sum("IS_REPEAT").alias("NUM_REPEAT_CUSTOMERS")
).withColumn(
    "REPEAT_ORDER_RATE", col("NUM_REPEAT_CUSTOMERS") / col("NUM_CUSTOMERS")
)

# Write to gold layer
df_summary.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_path)

print("✅ mart_loyalty_program_impact written successfully.")


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

