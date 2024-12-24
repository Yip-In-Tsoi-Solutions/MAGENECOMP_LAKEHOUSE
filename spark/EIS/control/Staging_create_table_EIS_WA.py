from pyspark.sql import SparkSession
from airflow.models import Variable

WAREHOUSE = "s3a://staging" #storage destination
appName = "App_create_staging_table_wa"
iceberg_table = "staging.lakehouse.EIS"

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
    .config('spark.sql.catalog.staging', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.staging.type','hadoop')\
    .config('spark.sql.catalog.staging.warehouse',WAREHOUSE)\
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_PRODUCT_WA
  (
    PRODUCT_ID           	string,
	PROGRAM_NAME         	string,
	PRODUCT_TYPE         	string,
	START_DATE           	timestamp_ntz,
	LAST_DATE            	timestamp_ntz,
	STATUS               	string,
	NOTES                	string,
	MARKING_TYPE         	string,
	TRANSFER_TYPE        	string,
	CONTROL_COMPONENT_INV	string,
	PLAN_PRIORITY        	int,
	CUSTOMER_PRODUCT_NAME	string,
	DEFAULT_FOR_REMAIN   	string,
	QA_FORMAT_FORM       	string,
	WAFER_MAP_TYPE       	string,
	GL_PRODUCT_CODE      	string,
	PROGRAM_NAME_PRINT   	string,
	FLAG_PRINT_MOISTURE  	string,
	PREGEN_SEQ           	decimal(22,3),
	GLUE_HOUR_EFFECT     	int,
	FAMILY_PRODUCT_ID    	string,
	TYPE_PHASE           	string,
	LINE_METHOD          	string,
	CODE_R_ACC           	string,
	MERGE_CLEAR_TRAY     	string,
	EN_ADD_PROGRAM       	string,
	DATE_ADD_PROGRAM     	timestamp_ntz,
	IPQC_PRODUCT_COLOR   	string,
	SAE_PROJECT_CODE     	string,
	PRODUCT_GROUP        	string,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_PRODUCT_FAMILY_WA
  (
    FAMILY_PRODUCT_ID	string,
	FAMILY_NAME      	string,
	SORTING_TYPE     	string,
	STATUS           	string,
	FAMILY_FVMI      	string,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_PRODUCT_PART_WA
  (
    PRODUCT_PART_NO             	string,
	KR_PART_NO                  	string,
	PRODUCT_ID                  	string,
	PART_TYPE                   	string,
	ROUTE_NO                    	string,
	LAT_LOT_SIZE                	int,
	MAP_TRAY_ROW                	int,
	MAP_TRAY_COL                	int,
	MAX_TRAY                    	int,
	START_DATE                  	timestamp_ntz,
	LAST_DATE                   	timestamp_ntz,
	STATUS                      	string,
	NOTES                       	string,
	TAB                         	string,
	WIP_FA_RUNCOUNTER           	int,
	GROUP_CREATED               	string,
	GROUP_MODIFY                	string,
	PCS_PER_TRAY                	int,
	MAP_BOAT_ROW                	int,
	MAP_BOAT_COL                	int,
	MAP_STRIP_COL               	int,
	PART_TYPE_NAME              	string,
	WIP_ENTITY_TYPE             	string,
	EER_NO_DEFAULT              	string,
	LATPERFORMANCE_CPK_LIMIT    	decimal(7,4),
	PREMASS                     	string,
	MPT_PRINT_LABEL             	string,
	WW_PRODUCT_PART_NO          	string,
	PLAN_PRIORITY               	int,
	ERP_PRODUCT_PART_NO         	string,
	FOR_GEN_LAT                 	string,
	USE_CUSTOMERBUYOFF          	string,
	FOR_PRINT_TRAY              	string,
	FOR_PRINT_PACK              	string,
	FOR_PRINT_MARKING           	string,
	NO_LIMIT_SUBMISSION         	string,
	LOCK_LIMIT_QTY_AT_FROMLATLOT	string,
	RUNNO_FORMAT                	string,
	PRODUCT_TYPE                	string,
	PRINT_STICKER_PACKING       	string,
	VISUAL_DEFECT_FLOW          	string,
	LOT_GROUPING_CONTROL        	string,
	PRINT_LABEL17_DIGIT         	string,
	MRR_LOT_GROUPING_CONTROL    	string,
	PRINT_LABEL13_17_DIGIT      	string,
	TRAY_TYPE_OF_BARCODE        	string,
	LAT_FORMAT                  	string,
	COLOR_LABEL                 	int,
	GL_PRODUCT_CODE             	string,
	USE_MINICORRELATION         	string,
	LOT_GROUPING_CONTROL_QTY    	string,
	FIRST_JOB_DIGIT             	string,
	MARK_RETURN                 	string,
	OSFM_RW_REV                 	string,
	ISPC_PRODUCT_PART_PREGEN    	string,
	FIRST_DIGIT_CODE            	string,
	TG_SITE                     	string,
	CONFIG_GEN_BARCODE_ID       	int,
	BOX_SIZE                    	int,
	LOT_SIZE                    	int,
	PACK_PER_BOX                	int,
	PRODUCT_PART_DESC           	string,
	TRAY_TYPE                   	string,
	PZT_SUPPLIER_CODE           	string,
	FLEX_SUPPLIER_CODE          	string,
	FIFO_CONTROL                	string,
	FIRST_COMP_GROUP            	string,
	MOUNT_PLANT_PLANE           	string,
	LOT_PATTERN                 	string,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_PRODUCT_OVERALL_WA
  (
    PLANT            	string,
	PRODUCT_ID       	string,
	PROGRAM_NAME     	string,
	PRODUCT_PART_NO  	string,
	PART_TYPE_NAME   	string,
	GL_PRODUCT_CODE  	string,
	FAMILY_PRODUCT_ID	string,
	FAMILY_CHILD_ID  	string,
	PRODUCT_CR       	string,
	PROGRAM_NAME_CR  	string,
	PRODUCT_NAME     	string,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_JIT_CELL_WA
  (
    JIT_CELL_NO                 	string,
	LINE_NO                     	string,
	ROOM                        	string,
	JIT_CELL_DIGIT              	string,
	AREA_CODE                   	int,
	TRANSFER_DATE               	timestamp_ntz,
	TRANSFER_USER               	string,
	PREGEN_FOR_MMS_EXPORTDTSHIFT	string,
	CELL_NICK_NAME              	string,
	IMPLEMENT_ON                	string,
	LOCATION                    	string,
	NEW_UNIT                    	string,
	NEW_BOI                     	string,
	STATUS                      	string,
	OLD_UNIT                    	string,
	OLD_BOI                     	string,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_DOCUMENT_EER_WA
  (
    PRODUCT_PART_NO	string,
    JIT_CELL_NO	string,
    EERNO	string,
    INPUT_TYPE	string,
    CREATED_DATE	timestamp_ntz,
    DOCU_ID	string,
    PURPOSE	string,
    EXPECTED_INPUT	decimal(11,3),
    EXPECTED_OUTPUT	decimal(11,3),
    EXPECTED_SCRAP	decimal(11,3),
    EER_STATUS_DOC	string,
    CUSTOMER_ID	string,
    COST_CENTER	string,
    EER_TYPE	string,
    ACTUAL_INPUT	decimal(11,3),
    ACTUAL_OUTPUT	decimal(11,3),
    ACTUAL_SCRAP	decimal(11,3),
    CALCULATE_TIME	timestamp_ntz,
    EER_STATUS_AUTO	string,
    START_DATE_AUTO	timestamp_ntz,
    FINISH_DATE_AUTO	timestamp_ntz,
    FINISH_DATE_DOC	timestamp_ntz,
    SUSPENSION_COST	decimal(11,3),
    ITEM_NUMBER	string,
    MATERIAL_TYPE	string,
    UPDATED_DATE	timestamp_ntz,
    PO_NO	string,
    STATUS	string,
    EER_DESCRIPTION	string,
    BARCODE_DIGIT	string,
    KEYIN_WIP_SEND	string,
    INTERNAL_MARKING	string,
    LAT_LOT_TYPE	string,
    AREA_CODE	string,
    CONFIG_GEN_BARCODE_ID	int,
    REPORT_STATUS	string,
    LOCK_OQA	string,
    POPUP_SKIPAUDIT_KEYIN	string,
    TRAY_SIZE	int,
    WRB_LOCATION	string,
    COMP_ID	int,
    MANUAL_MATERIAL	string,
    USER_UPDATE	string,
    USE_KEYIN_GOOD_DEFECT	string,
    MAXIMUM_DEFECT	int,
    MINIMUM_DEFECT	int,
    SPECIAL_GROUP	string,
    USE_LATONTRAY	string,
    NO_VMI	string,
    LOT_TYPE_GROUPING	string,
    RUNNING	string,
    TOOLCRIB_STATUS	string,
    SUB_LOT_TYPE	string,
    MATERIAL_TYPE_PI	string,
    SCRAP_TO_MERGE	string,
    EER_FINISH_DATE	timestamp_ntz,
    CQE_PURPOSE	string,
    EERNO_PARENT	string,
    KAGRA_LABEL	string,     
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_MACHINE_WA
  (
    MACHINE_NO             	string,
	MACHINE_NAME           	string,
	MACHINE_MODEL          	string,
	MACHINE_NO_JIT         	string,
	OPERATION_NO           	string,
	JIT_CELL_NO            	string,
	MULTI_OPERATION        	int,
	PM_STATUS              	string,
	CALIBRATION            	string,
	MACHINE_SEQUENCE       	int,
	LOCATION               	string,
	MACHINE_TYPE           	string,
	ASSET_NO               	string,
	ASSY_DRAWING           	string,
	TRANSFER_DATE          	timestamp_ntz,
	TRANSFER_USER          	string,
	APP_TYPE               	string,
	WORKING_TIME           	string,
	MACHINE_LIVE_STATUS    	string,
	LIFE_DATE              	timestamp_ntz,
	DSR_NO                 	string,
	PLAN_PRODUCT_ID        	string,
	FLAG_TABLE_CAL_OUTPUT  	string,
	FLAG_USE_INK_MARK_SCRAP	string,
	UPH_TARGET             	int,
	NEED_CALCULATE_UPH_AUTO	string,
	DIE_NO                 	string,
	YIELD_TARGET           	decimal(5,2),
	UTILIZATION_TARGET     	decimal(5,2),
	RUNTAB                 	string,
	PRODUCT_PART_NO        	string,
	CHANGEDIE_DATE         	timestamp_ntz,
	ZONE                   	string,
	SPC_CRUNCHING          	string,
	EMPL_CODE              	string,
	SPC_ALERT_SPCRULE      	string,
	FIX_DIE                	string,
	DIE_PER_MC             	int,
	BOI_MARKING            	string,
	PROJECT_BOI_ID         	int,
	FULL_OPTION            	string,
	CLASS_CODE             	string,
	PHYSICAL_JIT_CELL_NO   	string,
	ASSET_FILTER_STATUS    	string,
	FLAG_FAR_TRANSFER      	string,
	DT_CODE_DEFAULT        	string,
	USE_STATION            	string,
	COUNT_REMEASURE        	int,
	MARKING_FOR_GROUPING   	string,
	FLAG_SCAN              	string,
	PHYSICAL_PLANT         	string,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_WORK_WEEK_WA
  (
    YEAR         	string,
	QUARTER      	string,
	MONTH        	string,
	WW           	string,
	FROM_DATE    	timestamp_ntz,
	TO_DATE      	timestamp_ntz,
	CWW          	string,
	CYEAR        	string,
	SAT_FROM_DATE	timestamp_ntz,
	SAT_TO_DATE  	timestamp_ntz,
	WW_ID        	int,
	HSG_YY       	string,
	HSG_WW       	string,
	SEAGATE_YY   	string,
	SEAGATE_WW   	string,
	WD_YY        	string,
	WD_WW        	string,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_WIP_FA_WA
  (
    PRODUCT_PART_NO   	string,
	RUN_NO            	string,
	OPERATION_NO      	string,
	ROUTING_SEQ       	int,
	INPUT             	decimal(15,5),
	OUTPUT            	decimal(15,5),
	SCRAP             	decimal(15,5),
	TRANSFER_DATE     	timestamp_ntz,
	REMARK            	string,
	EOH               	int,
	EMPL_CODE         	string,
	WIP_FA_GROUP      	string,
	WIP_ALREADY       	string,
	DATE_IN           	timestamp_ntz,
	DATE_OUT          	timestamp_ntz,
	COST_PER_UNIT     	decimal(7,2),
	MACHINE_NO        	string,
	OPERATION_NO_MAP  	string,
	OPERATION_GROUP   	string,
	DIE_NO            	string,
	REPAIR_COMPLETED  	string,
	PARTIAL_COMPLETE  	string,
	APPLICATION_ENTRY 	string,
	NUM_OF_INSPEC     	int,
	NUM_OF_REJECT     	int,
	DATE_SWIP         	timestamp_ntz,
	IP_ADDRESS        	string,
	SPC_STATUS        	string,
	DATE_CHECK_IN     	timestamp_ntz,
	OPERATION_CODE_MAP	string,
	NCMR_REMARK       	string,
	HOLD_ID           	int,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_WIP_FA_RUNCARD_WA
  (
    PRODUCT_PART_NO	string,
    RUN_NO	string,
    EER_NO	string,
    CREATED_DATE	timestamp_ntz,
    WIP_FA_GROUP	string,
    JIT_CELL_NO	string,
    RUNNO_FORMAT	string,
    REPAIR_COMPLETED	string,
    SKIPAUDIT_TYPE	string,
    STATUS	string,
    APP_NAME	string,
    MPT_PRODUCT_PART_NO	string,
    GROUP_CUST_ID	int,
    LOAD_NO	int,
    LOCK_AUDIT_SKIP	string,
    CHECKSUM	string,
    MRR_NO	string,
    CONFIG_GEN_BARCODE_ID	int,
    TRAY_SIZE	int,
    WIP_COMPLETED	string,
    PASS_CLEANLINESS	string,
    LOT_HOLD_CLEANLINESS	string,
    WAFER_MAP_TYPE	string,
    SPECIAL_GROUP	string,
    WIP_COMPLETE_STATUS	string,
    IGNORE_GROUPING	string,
    EMERGENCY	string,
    SPC_STATUS	string,
    DISCRETE	string,
    OPERATION_HOLD	string,
    MO_NO	string,
    PART_NO	string,
    TRC_NO	string,
    ENS_TYPE	string,
    GEN_NEWFLOW_STATUS	string,
    REMARK	string,
    RUN_PSEUDO	string,
    RUN_REFER	string,
    TGA_CODE	string,
    VMI_TYPE	string,
    RECOVERY	string,
    RECOVERY_TYPE	string,
    GROUP_NO	int,
    ENIG_DATE	timestamp_ntz,
    MO_NO_ORIGINAL	string,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_WIP_FA_START_OPERATION_WA
  (
    PRODUCT_PART_NO	string,
	EER_NO         	string,
	OPERATION_NO   	string,
	TYPE           	string,
	AUTO_GEN       	int,
	STATUS         	string,
	CONFIRM        	string,
	TYPE_DPPM      	string,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_WIP_FA_WIP_TRANS_WA
  (
    RUN_NO        	string,
	OPERATION_NO  	string,
	CYCLE_NO      	int,
	INPUT         	int,
	OUTPUT        	int,
	SCRAP         	int,
	TRANSFER_DATE 	timestamp_ntz,
	WIP_ALREADY   	string,
	LAT_LOT_NO    	string,
	ACCEPT_ALREADY	string,
	SUBMISSION    	int,
	USER_UPDATE   	string,
	OQA_SAMPLE    	int,
	VERSION       	string,
	OQA_DEFECT    	int,
	PCK           	int,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_ROUTING_OPERATION_WA
  (
    PRODUCT_PART_NO         	string,
	EER_NO                  	string,
	OPERATION_NO            	string,
	ROUTING_SEQ             	int,
	OPERATION_TYPE          	string,
	CHARACTER_TYPE          	string,
	OPERATION_GROUP         	string,
	USETO_PERFORMANCE_REPORT	string,
	COST_PER_UNIT           	decimal(7,2),
	TRENDYIELD_REPORT       	string,
	OPERATION_NO_MAP        	string,
	LIST_MC_BY_CELL         	string,
	REQUIRE_DATE_IN         	string,
	BYPASS_ENTRY            	string,
	OPERATION_CODE_MAP      	string,
	NOENS_HIDE              	int,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_OPERATION_WA
  (
    OPERATION_NO	string,
    AREA_NAME	string,
    OPERATION_NAME	string,
    DESCRIPTION	string,
    SHORT_NAME	string,
    OWNER	string,
    OPERATION_MATCHING	string,
    AREA_CODE	int,
    OPERATION_RUN_REWORK	string,
    SEQ	int,
    STATUS_MACHINE_UTILIZE	string,
    PLAN_PRIORITY	int,
    OPERATION_CRUNCH	string,
    GL_OPERATION_NO	string,
    OPERATION_CODE_MAP	string,
    OPERATION_COLOR	string,
    IGNORE_CROSS_MACHINE	string,
    OPERATION_GROUP	string,
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
  CREATE OR REPLACE TABLE {iceberg_table}.EIS_RUNCARD_CONVERT_WA
  (
    RUN_NO	string,
    FROM_RUN	string,
    CONVERTED_DATE  	timestamp_ntz,
    PRODUCT_PART_NO	string,
    EER_NO	string,
    CREATED_DATE	timestamp_ntz,
    JIT_CELL_NO	string,
    MPT_PRODUCT_PART_NO	string,
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