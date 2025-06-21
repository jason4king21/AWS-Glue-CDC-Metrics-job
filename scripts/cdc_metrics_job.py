%idle_timeout 2880
%glue_version 5.0
%worker_type G.1X
%number_of_workers 5
%connections "BISqlserverConn"

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, sum as _sum, to_date, lag, avg, max, when
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import boto3
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)



# Initialize
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
s3 = boto3.client("s3")

# Constants
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
        (lit(today_str).cast("date") - max("CREATION_TIME_UTC").cast("date")).alias("days_since_last"),
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
