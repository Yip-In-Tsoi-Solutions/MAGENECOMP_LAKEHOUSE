from datetime import date
import pendulum
from airflow import DAG
from airflow.providers.ezmeral.spark.operators.ezspark_submit import EzSparkSubmitOperator
from airflow.models import Variable
from airflow.utils.email import send_email

#Failure Alert Message
def failure_email_alert(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    # Customize email subject and HTML body
    subject = f"Airflow Alert: Task Failed in DAG {dag_id}"
    html_content = f"""
        <h3>Airflow Task Failure Alert</h3>
        <p><b>DAG:</b> {dag_id}</p>
        <p><b>Task:</b> {task_id}</p>
        <p><b>Execution Date:</b> {execution_date}</p>
        <p><b>Log URL:</b> <a href="{log_url}">{log_url}</a></p>
        <p>Please check the logs for more details.</p>
    """
    # Send the email
    send_email(to=["Arjong.Ja@yipintsoi.com,korakot.pu@yipintsoi.com, panjamapon.ka@yipintsoi.com"], subject=subject, html_content=html_content)

# Default arguments ที่ใช้สำหรับ DAG นี้
access_key=Variable.get('access_key')
secret_key=Variable.get('secret_key')
# S3 Connection Configuration
conf = {
    "spark.hadoop.fs.s3a.access.key": access_key,
    "spark.hadoop.fs.s3a.secret.key": secret_key,
    "spark.hadoop.fs.s3a.imp": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.com.amazonaws.services.s3.enableV4": "true",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.endpoint": "https://hpedffs-prd-01.tlnw.magnecomp.com:9000"
}
# Get the current date
current_date = date.today()
time_zone = "Asia/Bangkok"
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(current_date.year, current_date.month, current_date.day-1, tz=time_zone),
    'on_failure_callback': failure_email_alert,
    'email_on_retry': False,
    'retries': 0,
}
with DAG(
    dag_id="DATA_CON_TRANSACTION_FACT_VIEW_ADHOC",
    default_args=default_args,
    description='Rerun DTC FACT and Mart with parameter dtc_num and dtc_file_date',
    schedule_interval=None,
    catchup=False,
    tags=['DTC', 'rerun','fact','view'],
) as dag:
    FACT_DATA_CON_TRANSACTION_ADHOC = EzSparkSubmitOperator(
        spark_binary='spark-submit',
        application=f"s3a://notebooks/lakehouse/process/DTC/adhoc/FACT_DATA_CON_TRANSACTION_ADHOC.py",
        conn_id='spark_conn',
        task_id='FACT_DATA_CON_TRANSACTION_ADHOC',
        conf=conf,
        dag=dag
    )
    VIEW_DATA_CON_TRANSACTION_ADHOC = EzSparkSubmitOperator(
        spark_binary='spark-submit',
        application=f"s3a://notebooks/lakehouse/mart/DTC/adhoc/VW_DATA_CON_TRANSACTION_ADHOC.py",
        conn_id='spark_conn',
        task_id='VIEW_DATA_CON_TRANSACTION_ADHOC',
        conf=conf,
        dag=dag
    )
    FACT_DATA_CON_TRANSACTION_ADHOC >> VIEW_DATA_CON_TRANSACTION_ADHOC
