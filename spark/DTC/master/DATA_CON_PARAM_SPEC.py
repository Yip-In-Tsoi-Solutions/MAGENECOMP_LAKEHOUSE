from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel
from airflow.models import Variable

appName = "DATA_CON_PARAM_HEADER"
access_key = Variable.get('access_key')
secret_key = Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"

#filename
file="DATA_CON_PARAM_SPEC.csv"

staging_endpoint = "s3a://staging"
master_endpoint = "s3a://master"
#source path file
source_path = f"s3a://staging/DTC/DATA_CON_PARAM_SPEC/{file}"
iceberg_table = f"master.lakehouse.DTC.DATA_CON_PARAM_SPEC"
spark = (
    SparkSession.builder
    .master("local")\
    .appName(appName)\
    .config('spark.jars.packages', 
            'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31')\
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\
    .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')\
    .config('spark.sql.catalog.spark_catalog.type', 'hive')\
    .config('spark.sql.catalog.master', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.master.type','hadoop')\
    .config('spark.sql.catalog.master.warehouse', master_endpoint)\
    
    .config('spark.sql.catalog.staging', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.staging.type','hadoop')\
    .config('spark.sql.catalog.staging.warehouse', staging_endpoint)\
    
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", 2) \
    .config("spark.executor.instances", 4) \
    
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

#query INF_BATCH_ID
inf_batch_id_df = spark.sql("SELECT MAX(INF_BATCH_ID) as max_batch FROM staging.lakehouse.INF_BATCH_ID_CTRL").select("max_batch").collect()
inf_batch_id = inf_batch_id_df[0]['max_batch']

spark.sql("truncate table master.lakehouse.DTC.DATA_CON_PARAM_SPEC")

df = spark.read \
    .format("csv") \
    .option("delimiter", ",") \
    .option("header", "True") \
    .option("multiLine","True")\
    .load(source_path)

df.createOrReplaceTempView("temp_view_spec")

#Saving dataframe to iceberg table
spark.sql(f"""
    INSERT INTO {iceberg_table}
    SELECT 
        MachineNo, VariableName, cast(SpecLCL as int) as SpecLCL, cast(SpecUCL as int) as SpecUCL,
        CONCAT('REC_',uuid()) as REC_ID,
        'DATA_CON_PARAM_SPEC' as SOURCE_TABLE_NAME,
        {inf_batch_id} as INF_BATCH_ID,
        'master_DATA_CON_PARAM_SPEC' as INF_SESS_NAME,
        'admin' as DM_MODIFIED_BY,
        current_timestamp() as DM_MODIFIED_DATE,
        'admin' as DM_CREATED_BY,
        current_timestamp() as DM_CREATED_DATE,
        current_timestamp() as DM_DATE_FROM,
        current_timestamp() as DM_DATE_TO
        FROM temp_view_spec
""")

print(f"inserted are {df.count()}")

spark.stop()