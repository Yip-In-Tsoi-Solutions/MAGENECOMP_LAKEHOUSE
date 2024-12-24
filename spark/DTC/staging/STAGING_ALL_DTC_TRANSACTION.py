from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql.types import *
from pyspark import StorageLevel
from airflow.models import Variable
appName = "ALL_DTC"
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"

#sources Table
staging_endpoint = "s3a://staging"
source_path = f"s3a://staging/DTC/ALL_DTC/*.txt"
#Destination Table
iceberg_table =  f"staging.lakehouse.DTC.STAGING_DTC_TRANSACTION"

#Spark Job Configuration
spark = (
    SparkSession.builder
    .master("local") \
    .appName(appName)\
    .config('spark.jars.packages', 
            'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31')\
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\
    .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')\
    .config('spark.sql.catalog.spark_catalog.type', 'hive')\
    .config('spark.sql.catalog.staging', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.staging.type','hadoop')\
    .config('spark.sql.catalog.staging.warehouse', staging_endpoint)\
    .config("spark.sql.legacy.createHiveTableByDefault", "false")\
    .config("spark.executor.instances", "40") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "24g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .config("spark.driver.memory", "6g") \
    .config("spark.driver.memoryOverhead", "600m") \
    .config("spark.default.parallelism", "400") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "20") \
    .config("spark.dynamicAllocation.maxExecutors", "60") \
    .config("spark.dynamicAllocation.initialExecutors", "40") \
    .config("spark.speculation", "true") \
    .config("spark.speculation.multiplier", "1.5") \
    .config("spark.speculation.quantile", "0.75") \
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
setConfig.set("fs.s3a.connection.ssl.enabled", "false")
setConfig.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
setConfig.set("fs.s3a.endpoint", bucket_endpoint)

inf_batch_id_df = spark.sql("SELECT MAX(INF_BATCH_ID) as max_batch FROM staging.lakehouse.INF_BATCH_ID_CTRL").select("max_batch").collect()
inf_batch_id = inf_batch_id_df[0]['max_batch']


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
df = spark.readStream.format("text").load(source_path)

filtered_df = df.filter(col("value").contains("Collected"))

# ดึงส่วน JSON ออกจากบรรทัดที่กรองได้ (ใช้ regexp_extract เพื่อดึงข้อมูล JSON)
json_df = filtered_df.withColumn(
    "json_part",
    regexp_extract(col("value"), r'(?i)Collected:\s*(\{.*\})', 1)
)
# แปลง JSON จากคอลัมน์ json_part เป็น DataFrame โดยใช้ฟังก์ชัน from_json()
parsed_df = json_df.withColumn("json_data", from_json(col("json_part"), schema))
# แตก Array "Values" ออกมาเป็นหลายๆ แถว โดยอ้างอิงจาก json_data.Values
exploded_df = parsed_df.withColumn("exploded_values", explode(col("json_data.Values")))
# เลือกคอลัมน์ที่ต้องการและจัดเรียงชื่อคอลัมน์ให้สอดคล้องกัน
final_df = exploded_df.select(
    col("json_data.MachineName").alias("MACHINENAME"),
    from_utc_timestamp(col("json_data.CollectedDate"), "Asia/Bangkok").alias("COLLECTEDDATE"),
    col("json_data.CEID"),
    col("json_data.CollectionEventName").alias("COLLECTIONEVENTNAME"),
    col("json_data.RPTID"),
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
    )
final_df.printSchema()

query = final_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"s3a://staging/checkpoint/STAGING_ALL_DTC_TRANSACTION") \
    .partitionBy("MACHINENAME")\
    .start(iceberg_table)

query.awaitTermination()