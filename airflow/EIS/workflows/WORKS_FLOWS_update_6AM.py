from datetime import date
import pendulum
from airflow import DAG
from airflow.providers.ezmeral.spark.operators.ezspark_submit import EzSparkSubmitOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email

# Define the custom email template function
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
    send_email(to=["peerapon.li@yipintsoi.com,korakot.pu@yipintsoi.com"], subject=subject, html_content=html_content)


# Default arguments ที่ใช้สำหรับ DAG นี้
access_key = Variable.get('access_key')
secret_key = Variable.get('secret_key')
# Get the current date
current_date = date.today()
time_zone = "Asia/Bangkok"
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 10, 22, tz=time_zone),
    'on_failure_callback': failure_email_alert,
    'email_on_failure': False,
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
    "spark.port.maxRetries": "50"
}

with DAG(
    dag_id="WORKS_FLOWS_update_6AM",
    default_args=default_args,
    description='Run DAG everyday at 06:00',
    schedule_interval='0 6 * * *',
    catchup=False,
    concurrency=15,
) as dag:
    with TaskGroup("STAGING_WA_GROUP") as STAGING_WA_GROUP:
        EIS_RUNCARD_CONVERT_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_RUNCARD_CONVERT/EIS_RUNCARD_CONVERT_WA.py",
            conn_id='spark_conn',
            task_id='EIS_RUNCARD_CONVERT_WA',
            conf=config,
            dag=dag
        )
    with TaskGroup("STAGING_WH_GROUP") as STAGING_WH_GROUP:
        EIS_RUNCARD_CONVERT_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_RUNCARD_CONVERT/EIS_RUNCARD_CONVERT_WH.py",
            conn_id='spark_conn',
            task_id='EIS_RUNCARD_CONVERT_WH',
            conf=config,
            dag=dag
        )
    with TaskGroup("STAGING_WN_GROUP") as STAGING_WN_GROUP:
        EIS_RUNCARD_CONVERT_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_RUNCARD_CONVERT/EIS_RUNCARD_CONVERT_WN.py",
            conn_id='spark_conn',
            task_id='EIS_RUNCARD_CONVERT_WN',
            conf=config,
            dag=dag
        )
    UPDATE_SFC_RUNCARD_DIM_WA = EzSparkSubmitOperator(
        spark_binary='spark-submit',
        application=f"s3a://notebooks/lakehouse/master/DIM/SFC_RUNCARD_DIM/update/UPDATE_SFC_RUNCARD_DIM_WA.py",
        conn_id='spark_conn',
        task_id='UPDATE_SFC_RUNCARD_DIM_WA',
        conf=config,
        dag=dag
    )
    UPDATE_SFC_RUNCARD_DIM_WH = EzSparkSubmitOperator(
        spark_binary='spark-submit',
        application=f"s3a://notebooks/lakehouse/master/DIM/SFC_RUNCARD_DIM/update/UPDATE_SFC_RUNCARD_DIM_WH.py",
        conn_id='spark_conn',
        task_id='UPDATE_SFC_RUNCARD_DIM_WH',
        conf=config,
        dag=dag
    )
    UPDATE_SFC_RUNCARD_DIM_WN = EzSparkSubmitOperator(
        spark_binary='spark-submit',
        application=f"s3a://notebooks/lakehouse/master/DIM/SFC_RUNCARD_DIM/update/UPDATE_SFC_RUNCARD_DIM_WN.py",
        conn_id='spark_conn',
        task_id='UPDATE_SFC_RUNCARD_DIM_WN',
        conf=config,
        dag=dag
    )
    

    STAGING_WN_GROUP >> UPDATE_SFC_RUNCARD_DIM_WN
    STAGING_WH_GROUP >> UPDATE_SFC_RUNCARD_DIM_WH
    STAGING_WA_GROUP >> UPDATE_SFC_RUNCARD_DIM_WA
