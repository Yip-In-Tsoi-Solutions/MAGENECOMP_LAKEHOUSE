from pyspark.sql import SparkSession
from airflow.models import Variable

process_bucket = "s3a://process" #storage destination
appName = "App_create_process_table"
catalog = "process.lakehouse.FACT"

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
    .config('spark.sql.catalog.process', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.process.type','hadoop')\
    .config('spark.sql.catalog.process.warehouse',process_bucket)\
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
  CREATE OR REPLACE TABLE {catalog}.SFC_WIP_RUNCARD_FACT
  (
    DATE_KEY	int,
    LINE_NO_KEY	int,
    ROUTE_OPERATION_NO_KEY	int,
    RUNCARD_NO_KEY	int,
    MACHINE_NO_KEY	int,
    INPUT_QTY	decimal(15,5),
    OUTPUT_QTY	decimal(15,5),
    SCRAP_QTY	decimal(15,5),
    DATE_IN	timestamp_ntz,
    DATE_OUT	timestamp_ntz,
    ETL_DATE	timestamp_ntz,
    EN_OPERATE	string,
    REC_ID	string,
    SOURCE_TABLE_NAME	string,
    INF_BATCH_ID	int,
    INF_SESS_NAME	string,
    DM_MODIFIED_BY	string,
    DM_MODIFIED_DATE	timestamp_ntz,
    DM_CREATED_BY	string,
    DM_CREATED_DATE	timestamp_ntz,
    DM_DATE_FROM	timestamp_ntz,
    DM_DATE_TO	timestamp_ntz
  ) USING iceberg
  """
)

spark.stop()
