from pyspark.sql import SparkSession
from airflow.models import Variable

appName = "SFC_RUNCARD_DIM_WA_rerun"
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"
staging_bucket = "s3a://staging" # read from staging bucket
master_bucket = "s3a://master" # write to master bucket
master_table = "master.lakehouse.DIM.SFC_RUNCARD_DIM"

#Assign Variable
inf_batch_id=Variable.get("INF_BATCH_ID")
transfer_date_from=Variable.get("transfer_date_from")
transfer_date_to=Variable.get("transfer_date_to")

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
     DELETE FROM {master_table} WHERE CREATED_DATE >= cast('{transfer_date_from}' || ' 07:00:01' as timestamp)
        AND CREATED_DATE <= cast('{transfer_date_to}' || ' 07:00:00' as timestamp) AND PLANT='WH'
""")


df = spark.sql(f""" 
    SELECT 
        SS.RUN_NO,
        PD.PRODUCT_PART_NO_KEY, 
        SS.EER_NO,
        SS.LAT_LOT_TYPE,
        SS.CREATED_DATE,
        LD.LINE_NO_KEY,
        null as FROM_RUN,
        SS.ETL_DATE,
        SS.PLANT,
        CONCAT('REC_',uuid()) as REC_ID,
        'EIS_WIP_FA_RUNCARD_WH' as SOURCE_TABLE_NAME,
        {inf_batch_id} as INF_BATCH_ID,
        'master_SFC_RUNCARD_DIM_WH' as INF_SESS_NAME,
        'admin' as DM_MODIFIED_BY,
        current_timestamp() as DM_MODIFIED_DATE,
        'admin' as DM_CREATED_BY,
        current_timestamp() as DM_CREATED_DATE,
        current_timestamp() as DM_DATE_FROM,
        current_timestamp() as DM_DATE_TO
    FROM (
        SELECT
        	DISTINCT RUNCARD.PRODUCT_PART_NO ,
        	RUNCARD.EER_NO,
        	EER.LAT_LOT_TYPE,
        	RUNCARD.CREATED_DATE,
        	RUNCARD.RUN_NO, 
            RUNCARD.JIT_CELL_NO ,
            'WH' PLANT ,
            current_timestamp() ETL_DATE
            
        FROM
        	staging.lakehouse.EIS.EIS_WIP_FA_RUNCARD_WH RUNCARD
        INNER JOIN staging.lakehouse.EIS.EIS_DOCUMENT_EER_WH eer ON
        	RUNCARD.EER_NO = EER.EERNO
        	AND RUNCARD.PRODUCT_PART_NO = EER.PRODUCT_PART_NO
        	AND RUNCARD.JIT_CELL_NO = EER.JIT_CELL_NO
        WHERE RUNCARD.CREATED_DATE >= cast('{transfer_date_from}' || ' 07:00:01' as timestamp)
        and RUNCARD.CREATED_DATE <= cast('{transfer_date_to}' || ' 07:00:00' as timestamp)
        and (APP_NAME != 'PSUDO' OR APP_NAME IS NULL)
    ) SS
    INNER JOIN master.lakehouse.DIM.MFG_PRODUCT_DIM PD
    ON SS.PRODUCT_PART_NO = PD.PRODUCT_PART_NO and SS.PLANT = PD.PLANT
    INNER JOIN master.lakehouse.DIM.MFG_LINE_DIM LD
    ON SS.JIT_CELL_NO = LD.LINE_NO and SS.PLANT = LD.PLANT

""")
df.createOrReplaceTempView("SFC_RUNCARD_DIM_WH")

# print("Count record :",df.count())
count_insert_df = spark.sql(f"""
    SELECT count(1) as total_count
    FROM SFC_RUNCARD_DIM_WH s
    LEFT JOIN {master_table} t 
    ON t.RUNCARD_NO = s.RUNCARD_NO AND t.PLANT = s.PLANT
    WHERE t.RUNCARD_NO IS NULL
""").select("total_count").collect()
count_insert = count_insert_df[0]['total_count']
print("Count insert record :",count_insert)

# spark.sql(f"""
#     INSERT INTO {master_table} SELECT * FROM SFC_RUNCARD_DIM_WH
# """)
spark.sql(f"""
    WITH max_sequence AS (
        SELECT NVL(MAX(RUNCARD_NO_KEY),300000000) AS max_seq FROM {master_table} WHERE PLANT='WH'
    )
    MERGE INTO {master_table} t
    USING (SELECT * FROM SFC_RUNCARD_DIM_WA) s 
    ON t.RUNCARD_NO = s.RUNCARD_NO AND t.PLANT = s.PLANT
    WHEN NOT MATCHED 
        THEN INSERT (RUNCARD_NO_KEY,RUNCARD_NO,PRODUCT_PART_NO_KEY,EER_NO,LAT_LOT_TYPE,CREATED_DATE,LINE_NO_KEY,FROM_RUN,ETL_DATE,PLANT,REC_ID,SOURCE_TABLE_NAME,INF_BATCH_ID,INF_SESS_NAME,DM_MODIFIED_BY,DM_MODIFIED_DATE,DM_CREATED_BY,DM_CREATED_DATE,DM_DATE_FROM,DM_DATE_TO)
        VALUES ((SELECT max_seq from max_sequence)+row_Number() over (ORDER BY s.RUNCARD_NO),s.RUNCARD_NO,s.PRODUCT_PART_NO_KEY,s.EER_NO,s.LAT_LOT_TYPE,s.CREATED_DATE,s.LINE_NO_KEY,s.FROM_RUN,s.ETL_DATE,s.PLANT,s.REC_ID,s.SOURCE_TABLE_NAME,s.INF_BATCH_ID,s.INF_SESS_NAME,s.DM_MODIFIED_BY,s.DM_MODIFIED_DATE,s.DM_CREATED_BY,s.DM_CREATED_DATE,s.DM_DATE_FROM,s.DM_DATE_TO)
""")

spark.stop()