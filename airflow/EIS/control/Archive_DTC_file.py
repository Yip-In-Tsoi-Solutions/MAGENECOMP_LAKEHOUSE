from datetime import date
import pendulum
from airflow import DAG
from airflow.providers.ezmeral.spark.operators.ezspark_submit import EzSparkSubmitOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
# Default arguments ที่ใช้สำหรับ DAG นี้
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
sourceFile="move_file_archive.py"
# Get the current date
current_date = date.today()
time_zone = "Asia/Bangkok"
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(current_date.year, current_date.month, current_date.day-1, tz=time_zone),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}
with DAG(
    dag_id="Archive_DTC_file",
    default_args=default_args,
    description='Move DTC file FROM ALL_DTC to Archive',
    schedule_interval=None,
    
    catchup=False,
) as dag:
    task_Archive_DTC_file = EzSparkSubmitOperator(
        spark_binary='spark-submit',
        application=f"s3a://notebooks/lakehouse/control/{sourceFile}",
        conn_id='spark_conn',
        task_id='Archive_DTC_file',
        conf={
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.imp": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.com.amazonaws.services.s3.enableV4": "true",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.endpoint": "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"
        },
        dag=dag
    )
