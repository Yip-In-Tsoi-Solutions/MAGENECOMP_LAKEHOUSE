from pyspark.sql import SparkSession
from airflow.models import Variable

appName = "create_table_DATA_CON_PARAM_SPEC"
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"

staging_endpoint = "s3a://master"
iceberg_table = f"master.lakehouse.DTC"

spark = (
    SparkSession.builder
    .master("local") \
    .appName(appName)\
    .config('spark.jars.packages', 
            'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31')\
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\
    .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')\
    .config('spark.sql.catalog.spark_catalog.type', 'hive')\
    .config('spark.sql.catalog.master', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.master.type','hadoop')\
    .config('spark.sql.catalog.master.warehouse', staging_endpoint)\
    .config("spark.sql.legacy.createHiveTableByDefault", "false")\
    .getOrCreate()
)
sqlContext = SparkSession(spark)
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

spark.sql(f"""
    CREATE OR REPLACE TABLE {iceberg_table}.DATA_CON_PARAM_SPEC (
        MachineNo STRING,
        VariableName STRING,
        SpecLCL INT,
        SpecUCL INT,
        REC_ID STRING,
        SOURCE_TABLE_NAME STRING,
        INF_BATCH_ID INT,
        INF_SESS_NAME STRING,
        DM_MODIFIED_BY STRING,
        DM_MODIFIED_DATE TIMESTAMP_NTZ,
        DM_CREATED_BY STRING,
        DM_CREATED_DATE TIMESTAMP_NTZ,
        DM_DATE_FROM TIMESTAMP_NTZ,
        DM_DATE_TO TIMESTAMP_NTZ
    ) USING iceberg
""")