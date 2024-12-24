from pyspark.sql import SparkSession
from airflow.models import Variable

master_bucket = "s3a://master" #storage destination
appName = "App_create_master_table"
catalog = "master.lakehouse.DIM"

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
    .config('spark.sql.catalog.master', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.master.type','hadoop')\
    .config('spark.sql.catalog.master.warehouse',master_bucket)\
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
  CREATE OR REPLACE TABLE {catalog}.MFG_DATE_DIM
  (
    DATE_KEY      	int,
	DATE_BEGIN    	date,
	DATE_END      	date,
	WW            	string,
	MONTH         	string,
	QUARTER       	string,
	YEAR          	string,
	DATETIME_BEGIN	timestamp_ntz,
	DATETIME_END  	timestamp_ntz,
	TIME_BEGIN    	timestamp_ntz,
	TIME_END      	timestamp_ntz,
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

spark.sql(
  f""" 
  CREATE OR REPLACE TABLE {catalog}.MFG_PRODUCT_DIM
  (
    PRODUCT_PART_NO_KEY	int,
	PRODUCT_PART_NO    	string,
	MPT_PART_NO        	string,
	PRODUCT_NAME       	string,
	FAMILY_NAME        	string,
	PLANT              	string,
	TAB                	string,
	GL_PRODUCT_CODE    	string,
	FIRST_JOB_DIGIT    	string,
	TG_SITE            	string,
	PRODUCT_PART_DESC  	string,
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

spark.sql(
  f""" 
  CREATE OR REPLACE TABLE {catalog}.MFG_LINE_DIM
  (
    LINE_NO_KEY	int,
	LINE_NO    	string,
	ROOM       	string,
	LOCATION   	string,
	PLANT      	string,
	LINE_CODE  	string,
	LINE_DIGIT 	string,
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

spark.sql(
  f""" 
  CREATE OR REPLACE TABLE {catalog}.MFG_MACHINE_DIM
  (
    MACHINE_NO_KEY	int,
	MACHINE_NO    	string,
	MACHINE_NAME  	string,
	MACHINE_GROUP 	string,
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

spark.sql(
  f""" 
  CREATE OR REPLACE TABLE {catalog}.MFG_OPERATION_DIM
  (
    OPERATION_NO_KEY	int,
	OPERATION_NO    	string,
	OPERATION_NAME  	string,
	PLANT           	string,
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

spark.sql(
  f""" 
  CREATE OR REPLACE TABLE {catalog}.SFC_RUNCARD_DIM
  (
    RUNCARD_NO_KEY     	int,
	RUNCARD_NO         	string,
	PRODUCT_PART_NO_KEY	int,
	EER_NO             	string,
	LAT_LOT_TYPE       	string,
	CREATED_DATE       	timestamp_ntz,
	LINE_NO_KEY        	int,
	FROM_RUN           	string,
	ETL_DATE           	timestamp_ntz,
	PLANT              	string,
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

spark.sql(
  f""" 
  CREATE OR REPLACE TABLE {catalog}.MFG_ROUTING_OPERATION_DIM
  (
    ROUTE_OPERATION_NO_KEY	int,
	OPERATION_NO          	string,
	PRODUCT_PART_NO_KEY   	int,
	EER_NO                	string,
	OPERATION_NAME        	string,
	OPERATION_SEQ         	string,
	LOT_TYPE              	string,
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