from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel
from airflow.models import Variable

appName = "FACT_DATA_CON_TRANSACTION_RERUN"
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"

machine_name_rerun=Variable.get('dtc_machine_name_rerun')
date_rerun=Variable.get('dtc_date_rerun')

staging_endpoint = "s3a://staging"
master_endpoint = "s3a://master"
process_endpoint = "s3a://process"

iceberg_table = f"process.lakehouse.DTC.FACT_DATA_CON_TRANSACTION"
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
    .config('spark.sql.catalog.master.warehouse', master_endpoint)\
    
    .config('spark.sql.catalog.staging', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.staging.type','hadoop')\
    .config('spark.sql.catalog.staging.warehouse', staging_endpoint)\
    
    .config('spark.sql.catalog.process', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.process.type','hadoop')\
    .config('spark.sql.catalog.process.warehouse', process_endpoint)\
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

#get inf_batch_id
inf_batch_id_df = spark.sql("SELECT MAX(INF_BATCH_ID) as max_batch FROM staging.lakehouse.INF_BATCH_ID_CTRL").select("max_batch").collect()
inf_batch_id = inf_batch_id_df[0]['max_batch']

spark.sql(f""" 
          DELETE FROM {iceberg_table} 
          WHERE 1=1
          AND MachineNo = '{machine_name_rerun}' 
          AND Date = cast('{date_rerun}' as date) 
          """)

df = spark.sql(f""" 
    select  upper(substr(a.MACHINENAME,1,6)) as MachineNo, a.COLLECTEDDATE as CreateDate,
    cast(date_format(a.COLLECTEDDATE,'yyyy-MM-dd') as date) as Date, a.COLLECTEDDATE as Second, cast(date_format(a.COLLECTEDDATE,'HH') as int) as Time,
    case when cast(date_format(a.COLLECTEDDATE,'HH') as int) < 7 then cast(date_format(a.COLLECTEDDATE,'yyyy-MM-dd') as date)-1 else cast(date_format(a.COLLECTEDDATE,'yyyy-MM-dd') as date) end as Date_7,
    a.CEID as CeId, a.COLLECTIONEVENTNAME as CollectionEventName, a.RPTID as RptId, a.VID as Vid, a.NAME as VariableName, a.VALUES as Value,'N' as Status
    
    ,case when (b.SpecLCL is null or b.SpecLCL='') then 0 else b.SpecLCL end as SpecLCL
    ,case when (b.SpecUCL is null or b.SpecUCL = '') then 0 else b.SpecUCL end as SpecUCL,
                a.REC_ID, 
               'FACT_DATA_CON_TRANSACTION' as SOURCE_TABLE_NAME, 
               {inf_batch_id} as INF_BATCH_ID, 
               'process_FACT_DATA_CON_TRANSACTION' as INF_SESS_NAME, 
               'admin' as DM_MODIFIED_BY, 
               current_timestamp() as DM_MODIFIED_DATE, 
               'admin' as DM_CREATED_BY, 
               a.DM_CREATED_DATE as DM_CREATED_DATE, 
               current_timestamp() as DM_DATE_FROM, 
               current_timestamp() as DM_DATE_TO
    from  staging.lakehouse.DTC.STAGING_DTC_TRANSACTION a left join master.lakehouse.DTC.DATA_CON_PARAM_SPEC b on upper(substr(a.MACHINENAME,1,6)) = upper(b.MachineNo)
    and a.NAME = b.VariableName
    WHERE 1=1 
    AND upper(substr(a.MACHINENAME,1,6)) = '{machine_name_rerun}'
    AND cast(a.COLLECTEDDATE as date) = cast('{date_rerun}' as date)
"""
)
df.createOrReplaceTempView("SFC_DATA_CON_TRANSACTION_tmp")

count_before_insert = spark.table(iceberg_table).count()

#upsert
# spark.sql(
#     f"""
#     MERGE INTO {iceberg_table} a
#     USING (SELECT * FROM SFC_DATA_CON_TRANSACTION) b
#     ON a.MachineNo = b.MachineNo
#        AND a.CreateDate = b.CreateDate
#        AND a.Vid = b.Vid
#        AND a.VariableName = b.VariableName
#     WHEN NOT MATCHED THEN
#         INSERT (MachineNo,CreateDate, Date, Second, Time, Date_7, CeId,CollectionEventName,RptId,Vid,VariableName,Value,Status,SpecLCL,SpecUCL,REC_ID,SOURCE_TABLE_NAME,INF_BATCH_ID,INF_SESS_NAME,DM_MODIFIED_BY,DM_MODIFIED_DATE,DM_CREATED_BY,DM_CREATED_DATE, DM_DATE_FROM, DM_DATE_TO)
#         VALUES (b.MachineNo,b.CreateDate, b.Date, b.Second, b.Time, b.Date_7, b.CeId,b.CollectionEventName,b.RptId,b.Vid,b.VariableName,b.Value, b.Status, b.SpecLCL, b.SpecUCL, CONCAT('REC_', uuid()), 'FACT_DATA_CON_TRANSACTION', {inf_batch_id}, 'process_FACT_DATA_CON_TRANSACTION', 'admin', current_timestamp(), 'admin', current_timestamp(), current_timestamp(), current_timestamp())
#     """
# )

# spark.sql(f"INSERT INTO {iceberg_table} SELECT b.MachineNo,b.CreateDate, b.Date, b.Second, b.Time, b.Date_7, b.CeId,b.CollectionEventName,b.RptId,b.Vid,b.VariableName,b.Value, b.Status, b.SpecLCL, b.SpecUCL, CONCAT('REC_', uuid()), 'FACT_DATA_CON_TRANSACTION', {inf_batch_id}, 'process_FACT_DATA_CON_TRANSACTION', 'admin', current_timestamp(), 'admin', current_timestamp(), current_timestamp(), current_timestamp() from SFC_DATA_CON_TRANSACTION b")
spark.sql(f"INSERT INTO {iceberg_table} SELECT * FROM SFC_DATA_CON_TRANSACTION_tmp")

count_after_insert = spark.table(iceberg_table).count()

rows_inserted = count_after_insert - count_before_insert
print(f"Rows inserted: {rows_inserted}")

# spark.sql("TRUNCATE TABLE staging.lakehouse.DTC.STAGING_DTC_TRANSACTION_TEST")

spark.stop()