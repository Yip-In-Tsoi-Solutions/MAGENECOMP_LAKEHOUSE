from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql.types import *
from pyspark import StorageLevel
from airflow.models import Variable

appName = "STAGING_DTC_TRANSACTION_REDIS"
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"

#sources files path
staging_endpoint = "s3a://staging"
source_path = f"s3a://staging/DTC/redis/*.json"
iceberg_table = f"staging.lakehouse.DTC.STAGING_DTC_TRANSACTION_REDIS"
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
    .config('spark.sql.catalog.staging.warehouse',staging_endpoint)\
    .config("spark.sql.legacy.createHiveTableByDefault", "false")\
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", 2) \
    .config("spark.executor.instances", 4) \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.session.timeZone", "Asia/Bangkok") \
    .getOrCreate()
)
#Dont Show warning only error
spark.sparkContext.setLogLevel("ERROR")
setConfig = spark._jsc.hadoopConfiguration()
setConfig.set("spark.sql.session.timeZone", "Asia/Bangkok")
setConfig.set("fs.s3a.access.key", access_key)
setConfig.set("fs.s3a.secret.key", secret_key)
setConfig.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
setConfig.set("com.amazonaws.services.s3.enableV4", "true")
setConfig.set("fs.s3a.path.style.access", "true")
setConfig.set("fs.s3a.connection.ssl.enabled", "true")
setConfig.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
setConfig.set("fs.s3a.endpoint", bucket_endpoint)

#query INF_BATCH_ID
inf_batch_id_df = spark.sql("SELECT MAX(INF_BATCH_ID) as max_batch FROM staging.lakehouse.INF_BATCH_ID_CTRL").select("max_batch").collect()
inf_batch_id = inf_batch_id_df[0]['max_batch']
#Define Schema
schema = StructType([
    StructField("MachineName", StringType(), False),
    StructField("MDLN", StringType(), False),
    StructField("SOFTREV", StringType(), False),
    StructField("CollectedDate", TimestampType(), False),
    StructField("CEID", IntegerType(), False),
    StructField("CollectionEventName", StringType(), False),
    StructField("RPTID", IntegerType(), False),
    StructField("Values", ArrayType(StructType([
        StructField("VID", IntegerType(), False),
        StructField("Name", StringType(), False),
        StructField("Value", StringType(), False)
    ])), False)
])
df = spark.readStream.schema(schema).json(source_path)
exploded_df = df.withColumn("exploded_values", explode(col("Values")))
# เลือกคอลัมน์ที่ต้องการใน DataFrame สุดท้าย
transofrm_df = exploded_df.select(
    col("MachineName").alias("MACHINENAME"),
    col("CollectedDate").alias("COLLECTEDDATE"),
    col("CEID"),
    col("CollectionEventName").alias("COLLECTIONEVENTNAME"),
    col("RPTID"),
    col("exploded_values.VID").alias("VID"),
    col("exploded_values.Name").alias("NAME"),
    col("exploded_values.Value").alias("VALUES")
).withColumn(
        "REC_ID", concat(lit("REC_"), expr("uuid()"))
    ).withColumn(
        "SOURCE_TABLE_NAME", upper(substring("MACHINENAME", 0, 6))
    ).withColumn(
        "INF_BATCH_ID",lit(inf_batch_id)
    ).withColumn(
        "INF_SESS_NAME", concat(lit("staging_"), lit(appName))
    ).withColumn(
        "DM_MODIFIED_BY",lit("Admin")
    ).withColumn(
        "DM_MODIFIED_DATE", from_utc_timestamp(current_timestamp(), "Asia/Bangkok")
    ).withColumn(
        "DM_CREATED_BY",lit("Admin")
    ).withColumn(
        "DM_CREATED_DATE", from_utc_timestamp(current_timestamp(), "Asia/Bangkok")
    ).withColumn(
        "DM_DATE_FROM", from_utc_timestamp(current_timestamp(), "Asia/Bangkok")
    ).withColumn(
        "DM_DATE_TO", from_utc_timestamp(current_timestamp(), "Asia/Bangkok")
    ).withColumn(
        "CREATE_DATE", to_date(from_utc_timestamp(current_date(), "Asia/Bangkok"))
    )

query = transofrm_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"s3a://staging/checkpoint/STAGING_DTC_TRANSACTION_REDIS") \
    .start(iceberg_table)

query.awaitTermination()