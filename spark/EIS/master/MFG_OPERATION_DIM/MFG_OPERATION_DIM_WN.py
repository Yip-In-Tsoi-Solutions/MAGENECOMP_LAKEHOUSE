from pyspark.sql import SparkSession
from airflow.models import Variable

appName = "MFG_OPERATION_DIM_WN"
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"
staging_bucket = "s3a://staging" # read from staging bucket
master_bucket = "s3a://master" # write to master bucket
master_table = "master.lakehouse.DIM.MFG_OPERATION_DIM"

spark = (
     SparkSession.builder
     .master("local")\
     .appName(appName)\
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31')\
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\
    .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')\
    .config('spark.sql.catalog.spark_catalog.type', 'hive')\
    .config('spark.sql.catalog.staging', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.staging.type','hadoop')\
    .config('spark.sql.catalog.staging.warehouse',staging_bucket)\
    .config('spark.sql.catalog.master', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.master.type','hadoop')\
    .config('spark.sql.catalog.master.warehouse',master_bucket)\
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

df = spark.sql(f"""
   SELECT 
       TRIM(OPERATION_NO) OPERATION_NO, 
       TRIM(OPERATION_NAME) OPERATION_NAME,
       'WN' AS PLANT,
       CONCAT('REC_',uuid()) as REC_ID,
       'EIS_OPERATION_WN' as SOURCE_TABLE_NAME,
       {inf_batch_id} as INF_BATCH_ID,
       'master_MFG_OPERATION_DIM_WN' as INF_SESS_NAME,
       'admin' as DM_MODIFIED_BY,
       current_timestamp() as DM_MODIFIED_DATE,
       'admin' as DM_CREATED_BY,
       current_timestamp() as DM_CREATED_DATE,
       current_timestamp() as DM_DATE_FROM,
       current_timestamp() as DM_DATE_TO
   FROM staging.lakehouse.EIS.EIS_OPERATION_WN
""")
df.createOrReplaceTempView("MFG_OPERATION_DIM_WN")

count_insert_df = spark.sql(f"""
    SELECT count(1) as total_count
    FROM MFG_OPERATION_DIM_WN s
    LEFT JOIN {master_table} t 
    ON t.OPERATION_NO = s.OPERATION_NO and t.PLANT = s.PLANT
    WHERE t.OPERATION_NO IS NULL
""").select("total_count").collect()
count_insert = count_insert_df[0]['total_count']
print("Count insert record :",count_insert)


spark.sql(f"""
    WITH max_sequence AS (
        SELECT NVL(MAX(OPERATION_NO_KEY),600000000) AS max_seq FROM {master_table} WHERE PLANT='WN'
    )
    MERGE INTO {master_table} t 
    USING (SELECT * FROM MFG_OPERATION_DIM_WN) s 
    ON t.OPERATION_NO = s.OPERATION_NO and t.PLANT = s.PLANT
    WHEN NOT MATCHED
        THEN INSERT (OPERATION_NO_KEY,OPERATION_NO,OPERATION_NAME,PLANT,REC_ID,SOURCE_TABLE_NAME,INF_BATCH_ID,INF_SESS_NAME,DM_MODIFIED_BY,DM_MODIFIED_DATE,DM_CREATED_BY,DM_CREATED_DATE,DM_DATE_FROM,DM_DATE_TO)
        VALUES ((SELECT max_seq from max_sequence)+row_Number() over (ORDER BY s.OPERATION_NO),s.OPERATION_NO,s.OPERATION_NAME,s.PLANT,s.REC_ID,s.SOURCE_TABLE_NAME,s.INF_BATCH_ID,s.INF_SESS_NAME,s.DM_MODIFIED_BY,s.DM_MODIFIED_DATE,s.DM_CREATED_BY,s.DM_CREATED_DATE,s.DM_DATE_FROM,s.DM_DATE_TO)
""")

spark.stop()