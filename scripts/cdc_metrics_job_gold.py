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




silver_path = f"s3://{bucket}/data/silver/order_revenue/"
gold_path = f"s3://{bucket}/data/gold/mart_customer_churn_profile/"


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

