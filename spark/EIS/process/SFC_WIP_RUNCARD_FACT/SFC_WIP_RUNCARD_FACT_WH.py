from pyspark.sql import SparkSession
from airflow.models import Variable

appName = "SFC_WIP_RUNCARD_FACT_WH"
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"
staging_bucket = "s3a://staging" # read from staging bucket
master_bucket = "s3a://master" # read from master bucket
process_bucket = "s3a://process" # write to process bucket
iceberg_table = "process.lakehouse.FACT.SFC_WIP_RUNCARD_FACT"

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

inf_batch_id_df = spark.sql("SELECT MAX(INF_BATCH_ID) as max_batch FROM staging.lakehouse.INF_BATCH_ID_CTRL").select("max_batch").collect()
inf_batch_id = inf_batch_id_df[0]['max_batch']

df = spark.sql(f"""
    SELECT DD.DATE_KEY, LD.LINE_NO_KEY, ROD.ROUTE_OPERATION_NO_KEY, RD.RUNCARD_NO_KEY, MD.MACHINE_NO_KEY,
    SS.INPUT INPUT_QTY, SS.OUTPUT OUTPUT_QTY, SS.SCRAP SCRAP_QTY, SS.DATE_IN, SS.DATE_OUT, SS.ETL_DATE
    FROM
    (
    SELECT DISTINCT A.* 
    FROM ( SELECT DISTINCT case when A.TRANSFER_DATE >= cast(cast(cast(A.TRANSFER_DATE as date) as string) || ' 07:00:01' as timestamp)
            and  A.TRANSFER_DATE <= cast(cast(date_add(cast(A.TRANSFER_DATE as date),+1) as string) || ' 07:00:00' as timestamp)
            then cast(cast(cast(A.TRANSFER_DATE as date) as string) || ' 07:00:01' as timestamp)
            when A.TRANSFER_DATE <= cast(cast(cast(A.TRANSFER_DATE as date) as string) || ' 07:00:00' as timestamp)
            and  A.TRANSFER_DATE >= cast(cast(date_add(cast(A.TRANSFER_DATE as date),-1) as string) || ' 07:00:01' as timestamp)
            then cast(cast(date_add(cast(A.TRANSFER_DATE as date),-1) as string) || ' 07:00:01' as timestamp)
            end  DATE_BEGIN, 
          B.MPT_PRODUCT_PART_NO, 
          A.PRODUCT_PART_NO, 
          B.JIT_CELL_NO, 
          C.LAT_LOT_TYPE, 
          B.EER_NO,  A.RUN_NO , 
          A.OPERATION_NO, 
          ROUTING_SEQ, 
          A.MACHINE_NO,  
          NVL(INPUT,0) INPUT, 
          (NVL(INPUT,0) - NVL(SCRAP,0) ) OUTPUT, 
          NVL(SCRAP,0) SCRAP, 
          TRANSFER_DATE, 
          'WH' PLANT, 
          A.DATE_IN,  
          A.DATE_OUT, 
          current_date() AS ETL_DATE
      
          FROM staging.lakehouse.EIS.EIS_WIP_FA_WH A, 
                staging.lakehouse.EIS.EIS_WIP_FA_RUNCARD_WH B, 
                staging.lakehouse.EIS.EIS_DOCUMENT_EER_WH C     
          WHERE A.TRANSFER_DATE  BETWEEN   cast(cast(date_add(current_date(),-1) as string) || ' 07:00:01' as timestamp)
          AND   cast(cast(current_date() as string) || ' 07:00:00' as timestamp)     
          AND   A.RUN_NO = B.RUN_NO   
          AND   A.PRODUCT_PART_NO  = B.PRODUCT_PART_NO  
          AND   A.PRODUCT_PART_NO  = C.PRODUCT_PART_NO 
          AND   B.EER_NO = C.EERNO 
          AND   B.JIT_CELL_NO  = C.JIT_CELL_NO
          AND   A.OPERATION_NO NOT IN (SELECT DISTINCT OPERATION_NO    
    		         FROM staging.lakehouse.EIS.EIS_WIP_FA_START_OPERATION_WH   
                                        	         WHERE (PRODUCT_PART_NO, EER_NO ) IN ( SELECT DISTINCT PRODUCT_PART_NO, EERNO   
                                                                              			FROM   staging.lakehouse.EIS.EIS_DOCUMENT_EER_WH 
                                                                              			WHERE (CREATED_DATE > date_add(current_date(),-1000) AND CREATED_DATE < current_date())  
                                                                                     			AND   MATERIAL_TYPE IN ('WIP') 
     					AND   LAT_LOT_TYPE NOT IN ('MAT')  
     					AND   EERNO NOT IN ('DUMMY')
     					)   
     		AND TYPE IN ('LAT','PACK')  
    		AND LENGTH(OPERATION_NO) = 4 
    		) 
            AND NVL(INPUT,0) > 0 
            AND OUTPUT >= 0
     
      UNION ALL
      
      SELECT case when A.TRANSFER_DATE >= cast(cast(cast(A.TRANSFER_DATE as date) as string) || ' 07:00:01' as timestamp)
            and  A.TRANSFER_DATE <= cast(cast(date_add(cast(A.TRANSFER_DATE as date),+1) as string) || ' 07:00:00' as timestamp)
            then cast(cast(cast(A.TRANSFER_DATE as date) as string) || ' 07:00:01' as timestamp)
            when A.TRANSFER_DATE <= cast(cast(cast(A.TRANSFER_DATE as date) as string) || ' 07:00:00' as timestamp)
            and  A.TRANSFER_DATE >= cast(cast(date_add(cast(A.TRANSFER_DATE as date),-1) as string) || ' 07:00:01' as timestamp)
            then cast(cast(date_add(cast(A.TRANSFER_DATE as date),-1) as string) || ' 07:00:01' as timestamp)
            end  DATE_BEGIN,
      B.MPT_PRODUCT_PART_NO,
      A.PRODUCT_PART_NO,
      B.JIT_CELL_NO,
      C.LAT_LOT_TYPE,
      B.EER_NO,
      A.RUN_NO ,
      A.OPERATION_NO,
      ROUTING_SEQ,
      A.MACHINE_NO,
      NVL(INPUT,0) INPUT,
      (NVL(INPUT,0) - NVL(SCRAP,0) ) OUTPUT,
      NVL(SCRAP,0) SCRAP,
      TRANSFER_DATE,
      'WH' PLANT,
      A.DATE_IN,   
      A.DATE_OUT,
      current_date() AS ETL_DATE
      
      FROM staging.lakehouse.EIS.EIS_WIP_FA_WH A,
          staging.lakehouse.EIS.EIS_WIP_FA_RUNCARD_WH B,
      staging.lakehouse.EIS.EIS_DOCUMENT_EER_WH C  
      WHERE A.TRANSFER_DATE  BETWEEN    cast(cast(date_add(current_date(),-1) as string) || ' 07:00:01' as timestamp)
      AND  cast(cast(current_date() as string) || ' 07:00:00' as timestamp)  
      AND A.RUN_NO  = B.RUN_NO  
      AND A.PRODUCT_PART_NO   = B.PRODUCT_PART_NO  
      AND A.PRODUCT_PART_NO   = C.PRODUCT_PART_NO  
      AND B.EER_NO            = C.EERNO  
      AND B.JIT_CELL_NO       = C.JIT_CELL_NO  
      AND C.LAT_LOT_TYPE  IN ('MAT','UDP')  
      AND C.EERNO NOT   IN ('DUMMY')  
      AND C.MATERIAL_TYPE    IN ('WIP')  
      AND NVL(INPUT,0)  > 0  AND OUTPUT >= 0 
      
      UNION ALL    
      
        SELECT case when M.TRANSFER_DATE >= cast(cast(cast(M.TRANSFER_DATE as date) as string) || ' 07:00:01' as timestamp)
            and  M.TRANSFER_DATE <= cast(cast(date_add(cast(M.TRANSFER_DATE as date),+1) as string) || ' 07:00:00' as timestamp)
            then cast(cast(cast(M.TRANSFER_DATE as date) as string) || ' 07:00:01' as timestamp)
            when M.TRANSFER_DATE <= cast(cast(cast(M.TRANSFER_DATE as date) as string) || ' 07:00:00' as timestamp)
            and  M.TRANSFER_DATE >= cast(cast(date_add(cast(M.TRANSFER_DATE as date),-1) as string) || ' 07:00:01' as timestamp)
            then cast(cast(date_add(cast(M.TRANSFER_DATE as date),-1) as string) || ' 07:00:01' as timestamp)
            end  DATE_BEGIN,
        MPT_PRODUCT_PART_NO, PRODUCT_PART_NO, JIT_CELL_NO, LAT_LOT_TYPE,   
        EER_NO, RUN_NO,     
        CASE  WHEN OPERATION_NO = '480' THEN (SELECT  MAX(OPERATION_NO)  OPERATION_NO  
        FROM staging.lakehouse.EIS.EIS_WIP_FA_START_OPERATION_WH    
        WHERE M.PRODUCT_PART_NO = PRODUCT_PART_NO  
        AND   M.EER_NO = EER_NO   
        AND   STATUS   = 'ACTIVE'    
        AND   TYPE  = 'LAT'
        ) 
        WHEN OPERATION_NO ='490' THEN  (SELECT  MAX(OPERATION_NO)  OPERATION_NO 
        FROM staging.lakehouse.EIS.EIS_WIP_FA_START_OPERATION_WH    
        WHERE M.PRODUCT_PART_NO = PRODUCT_PART_NO      
        AND   M.EER_NO     = EER_NO    
        AND   STATUS    = 'ACTIVE'   
        AND   TYPE      = 'PACK'  )
         ELSE OPERATION_NO    END OPERATION_NO,  
          CASE   WHEN OPERATION_NO = '480' THEN 480     
                 WHEN OPERATION_NO = '490' THEN 490     ELSE 999  END ROUTING_SEQ ,
         '' MACHINE_NO,          
         INPUT, 
         OUTPUT, 
         SCRAP, 
         TRANSFER_DATE , 
         'WH' PLANT ,  
         M.DATE_IN,          
         M.DATE_OUT,  
         current_date() AS ETL_DATE          
         FROM (  SELECT DISTINCT B.MPT_PRODUCT_PART_NO, 
                 B.PRODUCT_PART_NO, 
         B.JIT_CELL_NO, 
         C.LAT_LOT_TYPE,             
         B.EER_NO, B.RUN_NO, 
         A.OPERATION_NO,             
         sum(nvl(A.INPUT,0)) INPUT, 
         sum(nvl(A.OUTPUT,0)) OUTPUT, 
         sum(nvl(A.SCRAP,0) + nvl(A.OQA_SAMPLE,0) + nvl(A.OQA_DEFECT,0) + nvl(A.PCK,0)) SCRAP ,             
         max(A.TRANSFER_DATE)  TRANSFER_DATE , 
         max(D.DATE_IN) DATE_IN  , 
         max(D.DATE_OUT) DATE_OUT            
         FROM  staging.lakehouse.EIS.EIS_WIP_FA_WIP_TRANS_WH A, 
              staging.lakehouse.EIS.EIS_WIP_FA_RUNCARD_WH B, 
          staging.lakehouse.EIS.EIS_DOCUMENT_EER_WH C, 
          staging.lakehouse.EIS.EIS_WIP_FA_WH D,  
          (SELECT OPERATION_NO,          
          CASE OPERATION_GROUP  WHEN 'LAT' THEN '480'  WHEN 'PACK' THEN '490'  ELSE OPERATION_GROUP END OPERATION_WIP_TRANS  
          FROM staging.lakehouse.EIS.EIS_OPERATION_WH           
          WHERE OPERATION_GROUP IN ('LAT', 'PACK')
          ) E       
        WHERE A.TRANSFER_DATE  BETWEEN cast(cast(date_add(current_date(),-1) as string) || ' 07:00:01' as timestamp)         
         AND  cast(cast(current_date() as string) || ' 07:00:00' as timestamp)            
         AND   A.OPERATION_NO IN ('480','490')            
         AND   A.RUN_NO  = B.RUN_NO 
         AND   A.RUN_NO  = D.RUN_NO    
         AND   D.OPERATION_NO   = E.OPERATION_NO
         AND   A.OPERATION_NO   = E.OPERATION_WIP_TRANS    
         AND   A.WIP_ALREADY  = 'Y'      
         AND   A.ACCEPT_ALREADY  = 'Y'      
         AND   A.RUN_NO    = B.RUN_NO   
         AND   B.PRODUCT_PART_NO  = C.PRODUCT_PART_NO            
         AND   B.EER_NO   = C.EERNO            
         AND   B.JIT_CELL_NO   = C.JIT_CELL_NO  
         GROUP BY B.MPT_PRODUCT_PART_NO, B.PRODUCT_PART_NO, B.JIT_CELL_NO, C.LAT_LOT_TYPE,    B.EER_NO, B.RUN_NO, A.OPERATION_NO 
         ) M
        ) A
    
    ) SS
    
    INNER JOIN master.lakehouse.DIM.MFG_DATE_DIM DD
    ON cast(SS.DATE_BEGIN as date) = cast(DD.DATE_BEGIN as date)
    INNER JOIN master.lakehouse.DIM.MFG_LINE_DIM LD
    ON SS.JIT_CELL_NO = LD.LINE_NO and SS.PLANT = LD.PLANT
    INNER JOIN master.lakehouse.DIM.MFG_PRODUCT_DIM PD
    ON SS.PRODUCT_PART_NO = PD.PRODUCT_PART_NO and SS.PLANT = PD.PLANT
    INNER JOIN master.lakehouse.DIM.MFG_ROUTING_OPERATION_DIM ROD
    ON SS.OPERATION_NO = ROD.OPERATION_NO and SS.EER_NO = ROD.EER_NO 
    and SS.LAT_LOT_TYPE = ROD.LOT_TYPE and SS.ROUTING_SEQ = ROD.OPERATION_SEQ 
    and PD.PRODUCT_PART_NO_KEY = ROD.PRODUCT_PART_NO_KEY
    INNER JOIN master.lakehouse.DIM.SFC_RUNCARD_DIM RD
    ON SS.RUN_NO = RD.RUNCARD_NO and PD.PRODUCT_PART_NO_KEY = RD.PRODUCT_PART_NO_KEY
    LEFT JOIN master.lakehouse.DIM.MFG_MACHINE_DIM MD
    ON SS.MACHINE_NO = MD.MACHINE_NO

""")
df.createOrReplaceTempView("SFC_WIP_RUNCARD_FACT_WH")

# print("Count record :",df.count())

spark.sql(f"""
     DELETE FROM {iceberg_table} WHERE INF_BATCH_ID={inf_batch_id} AND INF_SESS_NAME='process_SFC_WIP_RUNCARD_FACT_WH'
""")

count_before_insert = spark.table("process.lakehouse.FACT.SFC_WIP_RUNCARD_FACT").filter("INF_SESS_NAME = 'process_SFC_WIP_RUNCARD_FACT_WH'").count()

spark.sql(f"""
    INSERT INTO {iceberg_table}
    SELECT 
        *,
        null as EN_OPERATE,
        CONCAT('REC_',uuid()) as REC_ID,
        'SFC_WIP_RUNCARD_FACT_WH' as SOURCE_TABLE_NAME,
        {inf_batch_id} as INF_BATCH_ID,
        'process_SFC_WIP_RUNCARD_FACT_WH' as INF_SESS_NAME,
        'admim' as DM_MODIFIED_BY,
        current_timestamp() as DM_MODIFIED_DATE,
        'admim' as DM_CREATED_BY,
        current_timestamp() as DM_CREATED_DATE,
        current_timestamp() as DM_DATE_FROM,
        current_timestamp() as DM_DATE_TO
        FROM SFC_WIP_RUNCARD_FACT_WH
""")

count_after_insert = spark.table("process.lakehouse.FACT.SFC_WIP_RUNCARD_FACT").filter("INF_SESS_NAME = 'process_SFC_WIP_RUNCARD_FACT_WH'").count()

rows_inserted = count_after_insert - count_before_insert
print(f"Rows inserted: {rows_inserted}")

spark.stop()