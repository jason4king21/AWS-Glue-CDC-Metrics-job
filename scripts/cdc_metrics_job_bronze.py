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


