from datetime import date
import pendulum
from airflow import DAG
from airflow.providers.ezmeral.spark.operators.ezspark_submit import EzSparkSubmitOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
# Default arguments ที่ใช้สำหรับ DAG นี้
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
sourceFile="test_runcard_convert.py"
# Get the current date
current_date = date.today()
time_zone = "Asia/Bangkok"
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(current_date.year, current_date.month, current_date.day, tz=time_zone),
    'email' : ["korakot.pu@yipintsoi.com"],
    'email_on_failure': True,
    'email_on_retry': True,
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
    "spark.hadoop.fs.s3a.endpoint": "https://hpedffs-prd-01.tlnw.magnecomp.com:9000",
}
with DAG(
    dag_id="test_runcard_convert",
    default_args=default_args,
    description='Run DAG everyday at 7:00',
    schedule_interval='0 1 * * *', 
    catchup=False,
) as dag:
    INF_BATCH_ID_CTRL = EzSparkSubmitOperator(
        spark_binary='spark-submit',
        application=f"s3a://notebooks/lakehouse/control/{sourceFile}",
        conn_id='spark_conn',
        task_id='test_runcard_convert',
        conf=config,
        dag=dag
    )
