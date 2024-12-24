from pyspark.sql import SparkSession
from airflow.models import Variable

appName = "EIS_WIP_FA_WIP_TRANS_WH"
tableName = f"lakehouse.EIS.EIS_WIP_FA_WIP_TRANS_WH"
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"
bucketPath = "s3a://staging"
iceberg_table = f"staging.{tableName}"
# การกำหนดค่าการเชื่อมต่อ
connectionString = "jdbc:oracle:thin:@//TLNW-PRD-EISDB02vip.tlnw.magnecomp.com:1521/HTODB.tlnw.magnecomp.com?oracle.jdbc.timezoneAsRegion=false"
oracleUser=Variable.get("oracle_wh_user")
oraclePass=Variable.get("oracle_wh_password")

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
    .config('spark.sql.catalog.staging.warehouse',bucketPath)\
    .config("spark.sql.legacy.createHiveTableByDefault", "false")\
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", 2) \
    .config("spark.executor.instances", 4) \
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

inf_batch_id_df = spark.sql("SELECT MAX(INF_BATCH_ID) as max_batch FROM staging.lakehouse.INF_BATCH_ID_CTRL").select("max_batch").collect()
inf_batch_id = inf_batch_id_df[0]['max_batch']

df = spark.read\
    .format("jdbc") \
    .option("url", connectionString) \
    .option("dbtable", "EIS.EIS_WIP_FA_WIP_TRANS") \
    .option("user", oracleUser) \
    .option("password", oraclePass) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()
df.createOrReplaceTempView("EIS_WIP_FA_WIP_TRANS_WH")

print("Count record :",df.count())

spark.sql(f"""
    INSERT INTO {iceberg_table}
    SELECT 
        *,
        CONCAT('REC_',uuid()) as REC_ID,
        'EIS_WIP_FA_WIP_TRANS_WH' as SOURCE_TABLE_NAME,
        {inf_batch_id} as INF_BATCH_ID,
        'Staging_EIS_WIP_FA_WIP_TRANS_WH' as INF_SESS_NAME,
        'admin' as DM_MODIFIED_BY,
        current_timestamp() as DM_MODIFIED_DATE,
        'admin' as DM_CREATED_BY,
        current_timestamp() as DM_CREATED_DATE,
        current_timestamp() as DM_DATE_FROM,
        current_timestamp() as DM_DATE_TO
        FROM EIS_WIP_FA_WIP_TRANS_WH
        WHERE TRANSFER_DATE <= cast(cast(current_date() as string) || ' 07:00:00' as timestamp)
        AND NVL(INPUT, 0) > 0
    	AND OUTPUT >= 0
""")

spark.stop()