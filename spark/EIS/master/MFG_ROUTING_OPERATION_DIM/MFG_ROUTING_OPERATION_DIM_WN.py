from pyspark.sql import SparkSession
from airflow.models import Variable

appName = "MFG_ROUTING_OPERATION_DIM_WN"
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"
staging_bucket = "s3a://staging" # read from staging bucket
master_bucket = "s3a://master" # write to master bucket
master_table = "master.lakehouse.DIM.MFG_ROUTING_OPERATION_DIM"

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
   WITH opers AS (
        select ro.OPERATION_NO, ro.PRODUCT_PART_NO,ro.EER_NO, upper(ro.EER_NO) EER_NO_upper, o.OPERATION_NAME, 
        NVL(og.OPERATION_WIP_TRANS, ro.ROUTING_SEQ) ROUTING_SEQ,
        d.LAT_LOT_TYPE
        FROM staging.lakehouse.EIS.EIS_ROUTING_OPERATION_WN ro
        LEFT JOIN staging.lakehouse.EIS.EIS_OPERATION_WN o on ro.OPERATION_NO = o.OPERATION_NO
        LEFT JOIN staging.lakehouse.EIS.EIS_DOCUMENT_EER_WN d on ro.PRODUCT_PART_NO = d.PRODUCT_PART_NO and ro.EER_NO = d.EERNO
        LEFT JOIN (     
                SELECT  OPERATION_NO,
                    CASE OPERATION_GROUP
                      WHEN 'LAT'
                      THEN 480
                      WHEN 'PACK'
                      THEN 490
                      ELSE OPERATION_GROUP
                    END OPERATION_WIP_TRANS
                  FROM staging.lakehouse.EIS.EIS_OPERATION_WN
                  WHERE OPERATION_GROUP IN ('LAT', 'PACK')
          ) og 
          ON (og.OPERATION_NO = o.OPERATION_NO)
    )
    SELECT 
        SS.*,
        PD.PRODUCT_PART_NO_KEY,
        CONCAT('REC_',uuid()) as REC_ID,
        'EIS_ROUTING_OPERATION_WN' as SOURCE_TABLE_NAME,
        {inf_batch_id} as INF_BATCH_ID,
        'master_MFG_ROUTING_OPERATION_DIM_WN' as INF_SESS_NAME,
        'admin' as DM_MODIFIED_BY,
        current_timestamp() as DM_MODIFIED_DATE,
        'admin' as DM_CREATED_BY,
        current_timestamp() as DM_CREATED_DATE,
        current_timestamp() as DM_DATE_FROM,
        current_timestamp() as DM_DATE_TO
    FROM (
    SELECT AA.PRODUCT_PART_NO, AA.EER_NO, AA.ROUTING_SEQ OPERATION_SEQ, AA.OPERATION_NO, AA.LAT_LOT_TYPE LOT_TYPE,
    AA.PLANT, AA.OPERATION_NAME,
    BB.ROUTING_SEQ_START_YIELD,
    BB.ROUTING_SEQ_END_YIELD,
    BB.YIELD
    FROM(
        SELECT OPERATION_NO,PRODUCT_PART_NO ,max(EER_NO) EER_NO, max(OPERATION_NAME) OPERATION_NAME, ROUTING_SEQ, max(LAT_LOT_TYPE) LAT_LOT_TYPE,'WN' PLANT 
        FROM opers
        GROUP BY EER_NO_upper, OPERATION_NO, PRODUCT_PART_NO, routing_seq
        ) as AA
    
    LEFT JOIN
    (
    SELECT D.PRODUCT_PART_NO, D.EER_NO, D.ROUTING_SEQ, D.OPERATION_NO,
    E.ROUTING_SEQ ROUTING_SEQ_START_YIELD,
    F.ROUTING_SEQ ROUTING_SEQ_END_YIELD,
    CASE 
    WHEN(D.ROUTING_SEQ >= E.ROUTING_SEQ AND D.ROUTING_SEQ <= F.ROUTING_SEQ ) THEN 'YIELD'
    END AS YIELD
    FROM
    staging.lakehouse.EIS.EIS_ROUTING_OPERATION_WN D 
    LEFT JOIN ( SELECT S.TYPE, R.ROUTING_SEQ, R.OPERATION_NO, R.EER_NO, R.PRODUCT_PART_NO
                FROM staging.lakehouse.EIS.EIS_WIP_FA_START_OPERATION_WN S, staging.lakehouse.EIS.EIS_ROUTING_OPERATION_WN R 
                WHERE (S.PRODUCT_PART_NO = R.PRODUCT_PART_NO AND S.EER_NO = R.EER_NO AND S.OPERATION_NO = R.OPERATION_NO)
                AND S.TYPE IN ('YIELD')) E 
            ON (E.PRODUCT_PART_NO = D.PRODUCT_PART_NO AND E.EER_NO = D.EER_NO)
    LEFT JOIN ( SELECT S.TYPE, R.ROUTING_SEQ, R.OPERATION_NO, R.EER_NO, R.PRODUCT_PART_NO
                FROM staging.lakehouse.EIS.EIS_WIP_FA_START_OPERATION_WN S, staging.lakehouse.EIS.EIS_ROUTING_OPERATION_WN R 
                WHERE (S.PRODUCT_PART_NO = R.PRODUCT_PART_NO AND S.EER_NO = R.EER_NO AND S.OPERATION_NO = R.OPERATION_NO)
                AND S.TYPE IN ('ENDYI')) F 
            ON (F.PRODUCT_PART_NO = D.PRODUCT_PART_NO AND F.EER_NO = D.EER_NO) ) BB

    ON AA.PRODUCT_PART_NO = BB.PRODUCT_PART_NO
    AND AA.EER_NO = BB.EER_NO
    AND AA.OPERATION_NO = BB.OPERATION_NO
    ) SS
    LEFT JOIN master.lakehouse.DIM.MFG_PRODUCT_DIM PD
    ON SS.PRODUCT_PART_NO = PD.PRODUCT_PART_NO and SS.PLANT= PD.PLANT
""")

df.createOrReplaceTempView("MFG_ROUTING_OPERATION_DIM_WN")

count_insert_df = spark.sql(f"""
    SELECT count(1) as total_count
    FROM MFG_ROUTING_OPERATION_DIM_WN s
    LEFT JOIN {master_table} t 
    ON t.OPERATION_NO = s.OPERATION_NO and t.EER_NO = s.EER_NO and t.OPERATION_SEQ=s.OPERATION_SEQ and COALESCE(t.PRODUCT_PART_NO_KEY,0)=COALESCE(s.PRODUCT_PART_NO_KEY,0)
    WHERE t.OPERATION_NO IS NULL
""").select("total_count").collect()
count_insert = count_insert_df[0]['total_count']
print("Count insert record :",count_insert)

spark.sql(f"""
    WITH max_sequence AS (
        SELECT NVL(MAX(ROUTE_OPERATION_NO_KEY),100000000) AS max_seq FROM {master_table} WHERE INF_SESS_NAME='master_MFG_ROUTING_OPERATION_DIM_WN'
    )
    MERGE INTO {master_table} t 
    USING (SELECT * FROM MFG_ROUTING_OPERATION_DIM_WN) s 
    ON t.OPERATION_NO = s.OPERATION_NO and t.EER_NO = s.EER_NO and t.OPERATION_SEQ=s.OPERATION_SEQ and COALESCE(t.PRODUCT_PART_NO_KEY,0)=COALESCE(s.PRODUCT_PART_NO_KEY,0)
    WHEN NOT MATCHED
        THEN INSERT (ROUTE_OPERATION_NO_KEY,OPERATION_NO,PRODUCT_PART_NO_KEY,EER_NO,OPERATION_NAME,OPERATION_SEQ,LOT_TYPE,ROUTING_SEQ_START_YIELD,ROUTING_SEQ_END_YIELD,YIELD,        
        REC_ID,SOURCE_TABLE_NAME,INF_BATCH_ID,INF_SESS_NAME,DM_MODIFIED_BY,DM_MODIFIED_DATE,DM_CREATED_BY,DM_CREATED_DATE,DM_DATE_FROM,DM_DATE_TO)
        VALUES ((SELECT max_seq from max_sequence)+row_Number() over (ORDER BY s.OPERATION_NO),s.OPERATION_NO,s.PRODUCT_PART_NO_KEY,s.EER_NO,s.OPERATION_NAME,s.OPERATION_SEQ,s.LOT_TYPE,s.ROUTING_SEQ_START_YIELD,s.ROUTING_SEQ_END_YIELD,s.YIELD,s.REC_ID,s.SOURCE_TABLE_NAME,s.INF_BATCH_ID,s.INF_SESS_NAME,s.DM_MODIFIED_BY,s.DM_MODIFIED_DATE,s.DM_CREATED_BY,s.DM_CREATED_DATE,s.DM_DATE_FROM,s.DM_DATE_TO)
""")

spark.stop()