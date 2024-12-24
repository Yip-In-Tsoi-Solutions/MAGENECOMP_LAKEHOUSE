from pyspark.sql import SparkSession
from airflow.models import Variable

appName = "DATA_CON_TRANSACTION"
access_key = "Y7WNJ7X4TXE1LDU045JO9IKNXE90HQNACI0WMRSBHWQ41DM1VNHVR1FAK6K3YVEG4XMP98C"
secret_key = "4C285D49SAJ8LVQLX"
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"
mart_bucket = "s3a://mart" # read from mart bucket
master_bucket = "s3a://master" # write to master bucket
process_bucket = "s3a://process" # write to process bucket

spark = (
    SparkSession.builder
    .master("local") \
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
    .config("spark.memory.offHeap.enabled","true") \
    .config("spark.memory.offHeap.size","16g") \
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
CREATE OR REPLACE TABLE mart.lakehouse.DATA_CON_TRANSACTION
AS

with cte1 as (
    select
distinct b.MachineNo,a.CeId,a.CollectionEventName,a.RptId
,a.Vid,a.Status
,a.`Date`
,sum(cast(a.Value as decimal(10,2))) as Value
,CASE WHEN (LOWER(RTRIM(LTRIM(b.VariableName)))='bondforce' or LOWER(RTRIM(LTRIM(b.VariableName)))='bondforcefloat') then 'BondForceFloat' 
            WHEN (LOWER(RTRIM(LTRIM(b.VariableName)))='' or LOWER(RTRIM(LTRIM(b.VariableName))) IS NULL) then '0'
            WHEN (LOWER(RTRIM(LTRIM(b.VariableName)))='dispenserpressure') then 'DispenserPressure'
            WHEN (LOWER(RTRIM(LTRIM(b.VariableName)))='pbiquality') then 'PbiQuality'
            ELSE
              RTRIM(LTRIM(b.VariableName))
 END as VariableName
,CASE WHEN (UPPER(RTRIM(LTRIM(b.MachineNo)))='DTC128' OR UPPER(RTRIM(LTRIM(b.MachineNo)))='DTC147' OR UPPER(RTRIM(LTRIM(b.MachineNo)))='DTC148' OR UPPER(RTRIM(LTRIM(b.MachineNo)))='DTC149' 
                 and LOWER(RTRIM(LTRIM(b.VariableName)))='bondforcefloat' or LOWER(RTRIM(LTRIM(b.VariableName)))='pbiquality') then 60
        WHEN (UPPER(RTRIM(LTRIM(b.MachineNo)))<>'DTC128' OR UPPER(RTRIM(LTRIM(b.MachineNo)))<>'DTC147' OR UPPER(RTRIM(LTRIM(b.MachineNo)))<>'DTC148' OR UPPER(RTRIM(LTRIM(b.MachineNo)))<>'DTC149' 
                 and LOWER(RTRIM(LTRIM(b.VariableName)))='bondforcefloat') then 70
        ELSE
          100
   END as Trigger
,AVG(b.SpecLCL) as LCL
,AVG(b.SpecUCL) as UCL
,cast(AVG(cast(a.Value as numeric(10,2))) as numeric(10,2)) as MEAN
,cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2)) as SIGMA
,case when (AVG(b.SpecLCL) is null and AVG(b.SpecUCL) is null and cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2)) is null) then NULL 
         when (AVG(b.SpecLCL) = 0 and AVG(b.SpecUCL) = 0 and cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2)) = 0) then NULL
         when b.SpecLCL = 0 then cast(LEAST( (ABS(AVG(b.SpecUCL) - cast(AVG(cast(a.Value as numeric(10,2))) as numeric(10,2))) / (3*cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2))))   ,  (cast(AVG(cast(a.Value as numeric(10,2))) as numeric(10,2))-AVG(b.SpecLCL))/ (3*cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2)))  ) as numeric(10,2))

    when (3*cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2)))  = 0 then NULL
         else 
           
           cast((AVG(b.SpecUCL) - cast(AVG(cast(a.Value as numeric(10,2))) as numeric(10,2))) / (3*cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2))) as numeric(10,2))
           
END as CPK
,'170' as LCL_DOTSIZE
,'260' as UCL_DOTSIZE
,cast(AVG(cast(a.Value as numeric(10,2))) as numeric(10,2)) as MEAN_DOTSIZE
,cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2)) as SIGMA_DOTSIZE
,case when cast(AVG(cast(a.Value as numeric(10,2))) as numeric(10,2))> 0 

and (3*cast(STDDEV_POP(cast(a.Value as numeric(10,2)))as numeric(10,2))) <> 0 then 
      ABS( cast(LEAST( (ABS('260' - cast(AVG(cast(a.Value as numeric(10,2))) as numeric(10,2))) / (3*cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2))))   ,  (cast(AVG(cast(a.Value as numeric(10,2))) as numeric(10,2))-'170')/ (3*cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2)))  ) as numeric(10,2)) )
     --cast(LEAST( (ABS(AVG(b.SpecUCL) - cast(AVG(cast(a.Value as numeric(10,2))) as numeric(10,2))) / (3*cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2))))   ,  (cast(AVG(cast(a.Value as numeric(10,2))) as numeric(10,2))-AVG(b.SpecLCL))/ (3*cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2)))  ) as numeric(10,2))
     --cast(LEAST( ABS('260' - cast(AVG(cast(a.Value as numeric(10,2))) as numeric(10,2)) / (3*cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2))))   ,  ABS(  cast(AVG(cast(a.Value as numeric(10,2))) as numeric(10,2))-'170'/ (3*cast(STDDEV_POP(cast(a.Value as numeric(10,2))) as numeric(10,2))) ) ) as numeric(10,2))
      else
         NULL
           
END as CPK_DOTSIZE
from 
master.lakehouse.DTC.DATA_CON_PARAM_SPEC as b inner join process.lakehouse.DTC.FACT_DATA_CON_TRANSACTION as a
on b.MachineNo=a.MachineNo and b.VariableName=a.VariableName 
--where b.MachineNo = 'DTC155' and a.`Date` = '2024-11-19'
group by b.MachineNo,a.CeId,a.CollectionEventName,a.RptId,a.Vid,a.Status
,b.SpecLCL
,b.VariableName
,a.`Date`
)

select * from cte1;

""")