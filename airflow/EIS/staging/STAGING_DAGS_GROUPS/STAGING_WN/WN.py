from datetime import date
import pendulum
from airflow import DAG
from airflow.providers.ezmeral.spark.operators.ezspark_submit import EzSparkSubmitOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Default arguments ที่ใช้สำหรับ DAG นี้
access_key = Variable.get('access_key')
secret_key = Variable.get('secret_key')
# Get the current date
current_date = date.today()
time_zone = "Asia/Bangkok"
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(current_date.year, current_date.month, current_date.day, tz=time_zone),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}
config = {
    "spark.hadoop.fs.s3a.access.key": access_key,
    "spark.hadoop.fs.s3a.secret.key": secret_key,
    "spark.hadoop.fs.s3a.imp": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.com.amazonaws.services.s3.enableV4": "true",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.endpoint": "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"
}

with DAG(
    dag_id="STAGING_WN",
    default_args=default_args,
    description='Run DAG everyday at 7:00',
    schedule_interval='0 7 * * *',
    catchup=False,
) as dag:
    with TaskGroup("WA_GROUP") as WA_GROUP:
        EIS_DOCUMENT_EER_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_DOCUMENT_EER/EIS_DOCUMENT_EER_WN.py",
            conn_id='spark_conn',
            task_id='EIS_DOCUMENT_EER_WN',
            conf=config,
            dag=dag
        )
        EIS_JIT_CELL_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_JIT_CELL/EIS_JIT_CELL_WN.py",
            conn_id='spark_conn',
            task_id='EIS_JIT_CELL_WN',
            conf=config,
            dag=dag
        )
        EIS_MACHINE_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_MACHINE/EIS_MACHINE_WN.py",
            conn_id='spark_conn',
            task_id='EIS_MACHINE_WN',
            conf=config,
            dag=dag
        )
        EIS_OPERATION_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_OPERATION/EIS_OPERATION_WN.py",
            conn_id='spark_conn',
            task_id='EIS_OPERATION_WN',
            conf=config,
            dag=dag
        )
        EIS_PRODUCT_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_PRODUCT/EIS_PRODUCT_WN.py",
            conn_id='spark_conn',
            task_id='EIS_PRODUCT_WN',
            conf=config,
            dag=dag
        )
        EIS_PRODUCT_FAMILY_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_PRODUCT_FAMILY/EIS_PRODUCT_FAMILY_WN.py",
            conn_id='spark_conn',
            task_id='EIS_PRODUCT_FAMILY_WN',
            conf=config,
            dag=dag
        )
        EIS_PRODUCT_OVERALL_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_PRODUCT_OVERALL/EIS_PRODUCT_OVERALL_WN.py",
            conn_id='spark_conn',
            task_id='EIS_PRODUCT_OVERALL_WN',
            conf=config,
            dag=dag
        )
        EIS_PRODUCT_PART_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_PRODUCT_PART/EIS_PRODUCT_PART_WN.py",
            conn_id='spark_conn',
            task_id='EIS_PRODUCT_PART_WN',
            conf=config,
            dag=dag
        )
        EIS_ROUTING_OPERATION_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_ROUTING_OPERATION/EIS_ROUTING_OPERATION_WN.py",
            conn_id='spark_conn',
            task_id='EIS_ROUTING_OPERATION_WN',
            conf=config,
            dag=dag
        )
        EIS_WIP_FA_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA/EIS_WIP_FA_WN.py",
            conn_id='spark_conn',
            task_id='EIS_WIP_FA_WN',
            conf=config,
            dag=dag
        )
        EIS_WIP_FA_RUNCARD_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA_RUNCARD/EIS_WIP_FA_RUNCARD_WN.py",
            conn_id='spark_conn',
            task_id='EIS_WIP_FA_RUNCARD_WN',
            conf=config,
            dag=dag
        )
        EIS_WIP_FA_START_OPERATION_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA_START_OPERATION/EIS_WIP_FA_START_OPERATION_WN.py",
            conn_id='spark_conn',
            task_id='EIS_WIP_FA_START_OPERATION_WN',
            conf=config,
            dag=dag
        )
        EIS_WIP_FA_WIP_TRANS_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA_WIP_TRANS/EIS_WIP_FA_WIP_TRANS_WN.py",
            conn_id='spark_conn',
            task_id='EIS_WIP_FA_WIP_TRANS_WN',
            conf=config,
            dag=dag
        )
        EIS_WORK_WEEK_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WORK_WEEK/EIS_WORK_WEEK_WN.py",
            conn_id='spark_conn',
            task_id='EIS_WORK_WEEK_WN',
            conf=config,
            dag=dag
        )
        [EIS_DOCUMENT_EER_WN , EIS_JIT_CELL_WN , EIS_MACHINE_WN , EIS_OPERATION_WN , EIS_PRODUCT_WN , EIS_PRODUCT_FAMILY_WN,
        EIS_PRODUCT_OVERALL_WN , EIS_PRODUCT_PART_WN , EIS_ROUTING_OPERATION_WN , EIS_WIP_FA_WN , EIS_WIP_FA_RUNCARD_WN,
        EIS_WIP_FA_START_OPERATION_WN , EIS_WIP_FA_WIP_TRANS_WN , EIS_WORK_WEEK_WN]
