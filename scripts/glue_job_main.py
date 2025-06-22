from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from datetime import datetime

# Initialize Spark and Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("daily_metrics_job", {})

# Log start time
print(f"Starting daily metrics job at {{datetime.now()}}")

# Import and run each metric script
import metrics.ltv
import metrics.rfm
import metrics.churn
import metrics.trend

# Log completion
print(f"Completed daily metrics job at {{datetime.now()}}")

job.commit()