from pyspark.sql import SparkSession
from airflow.models import Variable

appName = "CUMULATIVE_YIELD"
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"
mart_bucket = "s3a://mart" # read from mart bucket
master_bucket = "s3a://master" # write to master bucket
process_bucket = "s3a://process" # write to process bucket

spark = (
     SparkSession.builder
     .master("local")\
	 .appName(appName)\
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31')\
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\
    .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')\
    .config('spark.sql.catalog.spark_catalog.type', 'hive')\
    .config('spark.sql.catalog.mart', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.mart.type','hadoop')\
    .config('spark.sql.catalog.mart.warehouse',mart_bucket)\
    .config('spark.sql.catalog.master', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.master.type','hadoop')\
    .config('spark.sql.catalog.master.warehouse',master_bucket)\
    .config('spark.sql.catalog.process', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.process.type','hadoop')\
    .config('spark.sql.catalog.process.warehouse',process_bucket)\
    .config("spark.sql.legacy.createHiveTableByDefault", "false")\
    .config("spark.executor.instances", "40") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "12g") \
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


spark.sql(f"""
CREATE OR REPLACE TABLE mart.lakehouse.CUMULATIVE_YIELD AS 
SELECT A.DATE_BEGIN,A.WW,A.MMM,A.QUARTER,A.PRODUCT_FAMILY,A.MPT_PART_NO,A.LINE_NO,A.LAT_LOT_TYPE,A.OPERATION_NO,A.INPUT_QTY,A.OUTPUT_QTY,A.SCRAP_QTY,
CAST((CAST(A.OUTPUT_QTY  as DECIMAL(15,5))/ CAST(A.INPUT_QTY as DECIMAL(15,5))) * 100 as DECIMAL(15,2))as OPERATION_YIELD, 
CAST(EXP(SUM(LOG(CAST(A.OUTPUT_QTY as DECIMAL(15,5))/CAST(A.INPUT_QTY as DECIMAL(15,5)))) OVER(PARTITION BY A.DATE_BEGIN, A.LINE_NO, A.MPT_PART_NO, A.LAT_LOT_TYPE)) * 100 as DECIMAL(15,2)) as CUM_YIELD
FROM
(
	SELECT 
        	DD.DATE_BEGIN, DD.WW, 
            CONCAT(RIGHT(DD.YEAR,2),
            CASE WHEN MONTH(DD.DATE_BEGIN) =1 THEN 'JAN' 
            WHEN MONTH(DD.DATE_BEGIN) =2 THEN 'FEB' 
            WHEN MONTH(DD.DATE_BEGIN) =3 THEN 'MAR' 
            WHEN MONTH(DD.DATE_BEGIN) =4 THEN 'APR' 
            WHEN MONTH(DD.DATE_BEGIN) =5 THEN 'MAY' 
            WHEN MONTH(DD.DATE_BEGIN) =6 THEN 'JUN' 
            WHEN MONTH(DD.DATE_BEGIN) =7 THEN 'JUL' 
            WHEN MONTH(DD.DATE_BEGIN) =8 THEN 'AUG' 
            WHEN MONTH(DD.DATE_BEGIN) =9 THEN 'SEP' 
            WHEN MONTH(DD.DATE_BEGIN) =10 THEN 'OCT' 
            WHEN MONTH(DD.DATE_BEGIN) =11 THEN 'NOV' 
            WHEN MONTH(DD.DATE_BEGIN) =12 THEN 'DEC' END) AS MMM,
            RIGHT(DD.QUARTER,1) AS QUARTER,
            PD.FAMILY_NAME AS PRODUCT_FAMILY,
            PD.MPT_PART_NO,
            LD.LINE_NO,
            ROD.LOT_TYPE LAT_LOT_TYPE,
            ROD.OPERATION_NO,
			SUM(RF.INPUT_QTY) as INPUT_QTY, 
			SUM(RF.OUTPUT_QTY) as OUTPUT_QTY, 
			SUM(RF.SCRAP_QTY) as SCRAP_QTY
	  FROM process.lakehouse.FACT.SFC_WIP_RUNCARD_FACT AS RF
        INNER JOIN master.lakehouse.DIM.MFG_ROUTING_OPERATION_DIM AS ROD
        ON RF.ROUTE_OPERATION_NO_KEY = ROD.ROUTE_OPERATION_NO_KEY
        INNER JOIN master.lakehouse.DIM.MFG_PRODUCT_DIM PD
        ON ROD.PRODUCT_PART_NO_KEY = PD.PRODUCT_PART_NO_KEY
        INNER JOIN master.lakehouse.DIM.MFG_LINE_DIM LD
        ON RF.LINE_NO_KEY = LD.LINE_NO_KEY
        INNER JOIN master.lakehouse.DIM.MFG_DATE_DIM DD
        ON RF.DATE_KEY = DD.DATE_KEY
        WHERE ROD.YIELD = 'YIELD'
	  GROUP BY 
        DD.DATE_BEGIN, DD.WW, 
        CONCAT(RIGHT(DD.YEAR,2),
        CASE WHEN MONTH(DD.DATE_BEGIN) =1 THEN 'JAN' 
        WHEN MONTH(DD.DATE_BEGIN) =2 THEN 'FEB' 
        WHEN MONTH(DD.DATE_BEGIN) =3 THEN 'MAR' 
        WHEN MONTH(DD.DATE_BEGIN) =4 THEN 'APR' 
        WHEN MONTH(DD.DATE_BEGIN) =5 THEN 'MAY' 
        WHEN MONTH(DD.DATE_BEGIN) =6 THEN 'JUN' 
        WHEN MONTH(DD.DATE_BEGIN) =7 THEN 'JUL' 
        WHEN MONTH(DD.DATE_BEGIN) =8 THEN 'AUG' 
        WHEN MONTH(DD.DATE_BEGIN) =9 THEN 'SEP' 
        WHEN MONTH(DD.DATE_BEGIN) =10 THEN 'OCT' 
        WHEN MONTH(DD.DATE_BEGIN) =11 THEN 'NOV' 
        WHEN MONTH(DD.DATE_BEGIN) =12 THEN 'DEC' END),
        RIGHT(DD.QUARTER,1),
        PD.FAMILY_NAME ,
        PD.MPT_PART_NO,
        LD.LINE_NO,
        ROD.LOT_TYPE,
        ROD.OPERATION_NO
  ) A
  ORDER BY DATE_BEGIN, PRODUCT_FAMILY, MPT_PART_NO, LINE_NO, LAT_LOT_TYPE, OPERATION_NO
""")

spark.stop()