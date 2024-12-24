from pyspark.sql import SparkSession
from airflow.models import Variable

WAREHOUSE = "s3a://staging" #storage destination
appName = "App_control_inf_batch_id"
iceberg_table = "staging.lakehouse.INF_BATCH_ID_CTRL"

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
setConfig = spark._jsc.hadoopConfiguration()
setConfig.set("fs.s3a.access.key", access_key)
setConfig.set("fs.s3a.secret.key", secret_key)
setConfig.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
setConfig.set("com.amazonaws.services.s3.enableV4", "true")
setConfig.set("fs.s3a.path.style.access", "true")
setConfig.set("fs.s3a.connection.ssl.enabled", "false")
setConfig.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
setConfig.set("fs.s3a.endpoint", bucket_endpoint)
spark.sparkContext.setLogLevel("ERROR")

spark.sql(f"""
    WITH check_batch_id AS (
        SELECT NVL(MAX(INF_BATCH_ID),0) AS INF_BATCH_ID,
        cast(cast(date_add(current_date(),-1) as string) || ' 07:00:01' as timestamp) AS DATE_FROM, 
        cast(cast(current_date() as string) || ' 07:00:00' as timestamp) AS DATE_TO 
        FROM {iceberg_table}
        )
    MERGE INTO {iceberg_table} t 
    USING (SELECT * FROM check_batch_id) s 
        ON t.DATE_FROM = s.DATE_FROM
            AND t.DATE_TO = s.DATE_TO
    WHEN NOT MATCHED 
    THEN INSERT (INF_BATCH_ID,DATE_FROM,DATE_TO) VALUES (s.INF_BATCH_ID+1,s.DATE_FROM,s.DATE_TO)
""")

inf_batch_id_df = spark.sql("SELECT MAX(INF_BATCH_ID) as max_batch FROM staging.lakehouse.INF_BATCH_ID_CTRL").select("max_batch").collect()
inf_batch_id = inf_batch_id_df[0]['max_batch']

Variable.set('current_inf_batch_id', inf_batch_id)