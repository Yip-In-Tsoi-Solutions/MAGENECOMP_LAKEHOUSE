from pyspark.sql import SparkSession,DataFrame,Window
from pyspark.sql import functions as F
from airflow.models import Variable
from functools import reduce

appName = "UPDATE_SFC_RUNCARD_DIM_WA"
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
bucket_endpoint = "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"
staging_bucket = "s3a://staging" # read from staging bucket
master_bucket = "s3a://master" # write to master bucket
master_table = "master.lakehouse.DIM.SFC_RUNCARD_DIM"

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

inf_batch_id_df = spark.sql("SELECT MAX(INF_BATCH_ID) as max_batch FROM staging.lakehouse.INF_BATCH_ID_CTRL").select("max_batch").collect()
inf_batch_id = inf_batch_id_df[0]['max_batch']

start_date = F.concat_ws(" ", F.date_sub(F.current_date(), 1), F.lit("07:00:01")).cast("timestamp")
end_date = F.concat_ws(" ", F.current_date(), F.lit("07:00:00")).cast("timestamp")

# สร้าง DataFrame เริ่มต้นจาก RUNCARD_DIM และ JOIN กับ RUN_CONVERT เพื่อเริ่มการวนหา
df_runcard_dim = spark.table("master.lakehouse.DIM.SFC_RUNCARD_DIM").alias("rd").filter(F.col("PLANT") == 'WA').select(F.col("RUNCARD_NO_KEY"),F.col("RUNCARD_NO"))
df_run_convert = spark.table("staging.lakehouse.EIS.EIS_RUNCARD_CONVERT_WA") \
    .filter((F.col("CONVERTED_DATE") >= start_date) &
            (F.col("CONVERTED_DATE") <= end_date)) \
    .select(F.col("RUN_NO"),F.col("FROM_RUN"))

# JOIN SFC_RUNCARD_DIM และ EIS_RUNCARD_CONVERT
df_hierarchy = df_runcard_dim \
    .join(df_run_convert, df_runcard_dim["RUNCARD_NO"] == df_run_convert["FROM_RUN"]) \
    .select(
        df_runcard_dim["RUNCARD_NO_KEY"],
        df_runcard_dim["RUNCARD_NO"].alias("original_runcard_no"),
        df_run_convert["RUN_NO"].alias("current_run_no"),
        df_run_convert["FROM_RUN"].alias("from_run")
    ).alias("h")

dfs = [df_hierarchy]

# วนหา RUN_NO ต่อไปเรื่อย ๆ จนกว่าไม่มี FROM_RUN
while True:
    df_next = dfs[-1] \
        .join(df_run_convert.alias("rc_next"), dfs[-1]["current_run_no"] == F.col("rc_next.FROM_RUN"), "left") \
        .select(
            dfs[-1]["RUNCARD_NO_KEY"],
            dfs[-1]["original_runcard_no"],
            F.col("rc_next.RUN_NO").alias("current_run_no"),
            F.col("rc_next.FROM_RUN").alias("from_run")
        )
    
    df_next = df_next.filter(df_next.current_run_no.isNotNull())
    
    if df_next.count() == 0:
        break

    # เพิ่ม DataFrame ที่ได้เข้าไปในลิสต์
    dfs.append(df_next)

df_hierarchy = reduce(lambda df1, df2: df1.union(df2), dfs).distinct()

# เลือก RUN_NO สุดท้ายสำหรับแต่ละ original_runcard_no
window_spec = Window.partitionBy("original_runcard_no").orderBy(F.desc("current_run_no"))
df_final_run_no = df_hierarchy \
    .withColumn("rn", F.row_number().over(window_spec)) \
    .filter(F.col("rn") == 1) \
    .select("RUNCARD_NO_KEY","original_runcard_no", "current_run_no")

# แสดงผลลัพธ์
df_final_run_no.show()
df_final_run_no.createOrReplaceTempView("current_runcard_tmp")

spark.sql(f"""
    SELECT SS.RUNCARD_NO_KEY
          ,SS.original_runcard_no
          ,SS.current_run_no
          ,SS.CONVERTED_DATE 
          ,SS.EER_NO 
          ,SS.LAT_LOT_TYPE
          ,PD.PRODUCT_PART_NO_KEY
          ,LD.LINE_NO_KEY
    FROM (
        SELECT b.RUNCARD_NO_KEY,b.original_runcard_no,b.current_run_no
              ,a.CONVERTED_DATE 
              ,a.EER_NO 
              ,EER.LAT_LOT_TYPE
              ,a.PRODUCT_PART_NO 
              ,a.JIT_CELL_NO 
              ,'WA' AS PLANT
        FROM staging.lakehouse.EIS.EIS_RUNCARD_CONVERT_WA a
        INNER JOIN current_runcard_tmp b
        ON a.RUN_NO = b.current_run_no
        INNER JOIN staging.lakehouse.EIS.EIS_DOCUMENT_EER_WA EER 
        ON a.EER_NO = EER.EERNO
        AND a.PRODUCT_PART_NO = EER.PRODUCT_PART_NO
        AND a.JIT_CELL_NO = EER.JIT_CELL_NO
    ) SS
    INNER JOIN master.lakehouse.DIM.MFG_PRODUCT_DIM PD
    ON SS.PRODUCT_PART_NO = PD.PRODUCT_PART_NO and SS.PLANT = PD.PLANT
    INNER JOIN master.lakehouse.DIM.MFG_LINE_DIM LD
    ON SS.JIT_CELL_NO = LD.LINE_NO and SS.PLANT = LD.PLANT
""").createOrReplaceTempView("tmp_new_runcard")

count_update_df = spark.sql(f"""
    SELECT count(1) as total_count
    FROM master.lakehouse.DIM.SFC_RUNCARD_DIM t 
    INNER JOIN tmp_new_runcard s 
    ON t.RUNCARD_NO = s.original_runcard_no AND t.PLANT='WA'
""").select("total_count").collect()

count_update = count_update_df[0]['total_count']
print("Count update record :",count_update)

spark.sql(f"""
    MERGE INTO master.lakehouse.DIM.SFC_RUNCARD_DIM t
    USING (select * from tmp_new_runcard) s 
    ON t.RUNCARD_NO = s.original_runcard_no and t.RUNCARD_NO_KEY = s.RUNCARD_NO_KEY
    WHEN MATCHED AND t.PLANT='WA' THEN 
        UPDATE SET t.RUNCARD_NO = s.current_run_no,
                    t.CREATED_DATE = s.CONVERTED_DATE,
                    t.EER_NO = s. EER_NO,
                    t.FROM_RUN = s.original_runcard_no,
                    t.PRODUCT_PART_NO_KEY = s.PRODUCT_PART_NO_KEY,
                    t.LINE_NO_KEY = s.LINE_NO_KEY,
                    t.LAT_LOT_TYPE = s.LAT_LOT_TYPE
""")

spark.stop()