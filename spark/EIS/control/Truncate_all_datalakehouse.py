from pyspark.sql import SparkSession
from airflow.models import Variable

appName = "Truncate_all_datalakehouse"
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"
staging_bucket = "s3a://staging" # read from staging bucket
master_bucket = "s3a://master" # read from master bucket
process_bucket = "s3a://process" # write to process bucket

namespace_staging = 'staging.lakehouse.EIS'
namespace_master = 'master.lakehouse.DIM'
namespace_process = 'process.lakehouse.FACT'

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

inf_batch_id_df = spark.sql("SELECT MAX(INF_BATCH_ID) as max_batch FROM staging.lakehouse.INF_BATCH_ID_CTRL").select("max_batch").collect()
inf_batch_id = inf_batch_id_df[0]['max_batch']

#EIS_WORK_WEEK
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WORK_WEEK_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WORK_WEEK_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WORK_WEEK_WN")

#EIS_JIT_CELL
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_JIT_CELL_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_JIT_CELL_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_JIT_CELL_WN")

#EIS_PRODUCT
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_PRODUCT_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_PRODUCT_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_PRODUCT_WN")

#EIS_PRODUCT_OVERALL
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_PRODUCT_OVERALL_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_PRODUCT_OVERALL_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_PRODUCT_OVERALL_WN")

#EIS_PRODUCT_FAMILY
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_PRODUCT_FAMILY_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_PRODUCT_FAMILY_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_PRODUCT_FAMILY_WN")

#EIS_PRODUCT_PART
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_PRODUCT_PART_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_PRODUCT_PART_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_PRODUCT_PART_WN")

#EIS_DOCUMENT_EER
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_DOCUMENT_EER_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_DOCUMENT_EER_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_DOCUMENT_EER_WN")

#EIS_MACHINE
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_MACHINE_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_MACHINE_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_MACHINE_WN")

#EIS_OPERATION
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_OPERATION_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_OPERATION_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_OPERATION_WN")

#EIS_ROUTING_OPERATION
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_ROUTING_OPERATION_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_ROUTING_OPERATION_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_ROUTING_OPERATION_WN")

#EIS_WIP_FA
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WIP_FA_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WIP_FA_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WIP_FA_WN")

#EIS_WIP_FA_RUNCARD
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WIP_FA_RUNCARD_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WIP_FA_RUNCARD_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WIP_FA_RUNCARD_WN")

#EIS_WIP_FA_START_OPERATION
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WIP_FA_START_OPERATION_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WIP_FA_START_OPERATION_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WIP_FA_START_OPERATION_WN")

#EIS_WIP_FA_WIP_TRANS
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WIP_FA_WIP_TRANS_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WIP_FA_WIP_TRANS_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_WIP_FA_WIP_TRANS_WN")


#EIS_RUNCARD_CONVERT
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_RUNCARD_CONVERT_WA")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_RUNCARD_CONVERT_WH")
spark.sql(f"TRUNCATE TABLE {namespace_staging}.EIS_RUNCARD_CONVERT_WN")

#DIMENSION TABLE
#MFG_LINE_DIM
spark.sql(f"TRUNCATE TABLE {namespace_master}.MFG_LINE_DIM")

#MFG_MACHINE_DIM
spark.sql(f"TRUNCATE TABLE {namespace_master}.MFG_MACHINE_DIM")

#MFG_OPERATION_DIM
spark.sql(f"TRUNCATE TABLE {namespace_master}.MFG_OPERATION_DIM")

#MFG_PRODUCT_DIM
spark.sql(f"TRUNCATE TABLE {namespace_master}.MFG_PRODUCT_DIM")

#MFG_ROUTING_OPERATION_DIM
spark.sql(f"TRUNCATE TABLE {namespace_master}.MFG_ROUTING_OPERATION_DIM")

#SFC_RUNCARD_DIM
spark.sql(f"TRUNCATE TABLE {namespace_master}.SFC_RUNCARD_DIM")

#FACT
spark.sql(f"TRUNCATE TABLE {namespace_process}.SFC_WIP_RUNCARD_FACT")