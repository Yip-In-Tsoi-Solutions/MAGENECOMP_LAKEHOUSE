from pyspark.sql import SparkSession
from airflow.models import Variable

WAREHOUSE = "s3a://staging" #storage destination
appName = "App_create_table_ctrl_inf_batch_id"
iceberg_table = "staging.lakehouse.EIS"

access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint="https://hpedffs-prd-01.tlnw.magnecomp.com:9000"

spark = (
     SparkSession.builder
     .master("local") \
     .appName(appName)\
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31')\
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\
    .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')\
    .config('spark.sql.catalog.spark_catalog.type', 'hive')\
    .config('spark.sql.catalog.staging', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.staging.type','hadoop')\
    .config('spark.sql.catalog.staging.warehouse',WAREHOUSE)\
    .config("spark.sql.legacy.createHiveTableByDefault", "false")\
    .getOrCreate()
)
#Dont Show warning only error
spark.sparkContext.setLogLevel("ERROR")

setConfig = spark._jsc.hadoopConfiguration()
setConfig.set("fs.s3a.access.key", access_key)
setConfig.set("fs.s3a.secret.key", secret_key)
setConfig.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
setConfig.set("com.amazonaws.services.s3.enableV4", "true")
setConfig.set("fs.s3a.path.style.access", "true")
setConfig.set("fs.s3a.connection.ssl.enabled", "false")
setConfig.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
setConfig.set("fs.s3a.endpoint", bucket_endpoint)

spark.sql(
  f""" 
  CREATE OR REPLACE TABLE staging.lakehouse.INF_BATCH_ID_CTRL
  (
    INF_BATCH_ID int,
    DATE_FROM timestamp_ntz,
    DATE_TO timestamp_ntz
  )
 """)

spark.stop()