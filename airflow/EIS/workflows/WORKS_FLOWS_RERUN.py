from datetime import date
import pendulum
from airflow import DAG
from airflow.providers.ezmeral.spark.operators.ezspark_submit import EzSparkSubmitOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email
from airflow.operators.python_operator import PythonOperator

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

#test For stop all task initial
def failing_task():
    raise Exception("Intentional error for testing custom email alert.")


with DAG(
    dag_id="WORKS_FLOWS_RERUN",
    default_args=default_args,
    description='Rerun load data from transfer_date_from to transfer_date_to',
    schedule_interval=None,
    catchup=False,
    concurrency=15,
    tags=['cum_yield', 'rerun'],
) as dag:
    task1 = PythonOperator(
        task_id='failing_task',
        python_callable=failing_task
    )
    with TaskGroup("STAGING_WA_GROUP") as STAGING_WA_GROUP:
        EIS_DOCUMENT_EER_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_DOCUMENT_EER/EIS_DOCUMENT_EER_WA.py",
            conn_id='spark_conn',
            task_id='EIS_DOCUMENT_EER_WA',
            conf=config,
            dag=dag
        )
        EIS_JIT_CELL_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_JIT_CELL/EIS_JIT_CELL_WA.py",
            conn_id='spark_conn',
            task_id='EIS_JIT_CELL_WA',
            conf=config,
            dag=dag
        )
        EIS_MACHINE_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_MACHINE/EIS_MACHINE_WA.py",
            conn_id='spark_conn',
            task_id='EIS_MACHINE_WA',
            conf=config,
            dag=dag
        )
        EIS_OPERATION_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_OPERATION/EIS_OPERATION_WA.py",
            conn_id='spark_conn',
            task_id='EIS_OPERATION_WA',
            conf=config,
            dag=dag
        )
        EIS_PRODUCT_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_PRODUCT/EIS_PRODUCT_WA.py",
            conn_id='spark_conn',
            task_id='EIS_PRODUCT_WA',
            conf=config,
            dag=dag
        )
        EIS_PRODUCT_FAMILY_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_PRODUCT_FAMILY/EIS_PRODUCT_FAMILY_WA.py",
            conn_id='spark_conn',
            task_id='EIS_PRODUCT_FAMILY_WA',
            conf=config,
            dag=dag
        )
        EIS_PRODUCT_OVERALL_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_PRODUCT_OVERALL/EIS_PRODUCT_OVERALL_WA.py",
            conn_id='spark_conn',
            task_id='EIS_PRODUCT_OVERALL_WA',
            conf=config,
            dag=dag
        )
        EIS_PRODUCT_PART_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_PRODUCT_PART/EIS_PRODUCT_PART_WA.py",
            conn_id='spark_conn',
            task_id='EIS_PRODUCT_PART_WA',
            conf=config,
            dag=dag
        )
        EIS_ROUTING_OPERATION_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_ROUTING_OPERATION/EIS_ROUTING_OPERATION_WA.py",
            conn_id='spark_conn',
            task_id='EIS_ROUTING_OPERATION_WA',
            conf=config,
            dag=dag
        )
        EIS_WIP_FA_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA/rerun/EIS_WIP_FA_WA_rerun.py",
            conn_id='spark_conn',
            task_id='EIS_WIP_FA_WA',
            conf=config,
            dag=dag
        )
        EIS_WIP_FA_RUNCARD_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA_RUNCARD/EIS_WIP_FA_RUNCARD_WA.py",
            conn_id='spark_conn',
            task_id='EIS_WIP_FA_RUNCARD_WA',
            conf=config,
            dag=dag
        )
        EIS_WIP_FA_START_OPERATION_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA_START_OPERATION/EIS_WIP_FA_START_OPERATION_WA.py",
            conn_id='spark_conn',
            task_id='EIS_WIP_FA_START_OPERATION_WA',
            conf=config,
            dag=dag
        )
        EIS_WIP_FA_WIP_TRANS_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA_WIP_TRANS/rerun/EIS_WIP_FA_WIP_TRANS_WA_rerun.py",
            conn_id='spark_conn',
            task_id='EIS_WIP_FA_WIP_TRANS_WA',
            conf=config,
            dag=dag
        )
        EIS_WORK_WEEK_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WORK_WEEK/EIS_WORK_WEEK_WA.py",
            conn_id='spark_conn',
            task_id='EIS_WORK_WEEK_WA',
            conf=config,
            dag=dag
        )
    with TaskGroup("STAGING_WH_GROUP") as STAGING_WH_GROUP:
        EIS_DOCUMENT_EER_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_DOCUMENT_EER/EIS_DOCUMENT_EER_WH.py",
            conn_id='spark_conn',
            task_id='EIS_DOCUMENT_EER_WH',
            conf=config,
            dag=dag
        )
        EIS_JIT_CELL_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_JIT_CELL/EIS_JIT_CELL_WH.py",
            conn_id='spark_conn',
            task_id='EIS_JIT_CELL_WH',
            conf=config,
            dag=dag
        )
        EIS_MACHINE_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_MACHINE/EIS_MACHINE_WH.py",
            conn_id='spark_conn',
            task_id='EIS_MACHINE_WH',
            conf=config,
            dag=dag
        )
        EIS_OPERATION_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_OPERATION/EIS_OPERATION_WH.py",
            conn_id='spark_conn',
            task_id='EIS_OPERATION_WH',
            conf=config,
            dag=dag
        )
        EIS_PRODUCT_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_PRODUCT/EIS_PRODUCT_WH.py",
            conn_id='spark_conn',
            task_id='EIS_PRODUCT_WH',
            conf=config,
            dag=dag
        )
        EIS_PRODUCT_FAMILY_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_PRODUCT_FAMILY/EIS_PRODUCT_FAMILY_WH.py",
            conn_id='spark_conn',
            task_id='EIS_PRODUCT_FAMILY_WH',
            conf=config,
            dag=dag
        )
        EIS_PRODUCT_OVERALL_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_PRODUCT_OVERALL/EIS_PRODUCT_OVERALL_WH.py",
            conn_id='spark_conn',
            task_id='EIS_PRODUCT_OVERALL_WH',
            conf=config,
            dag=dag
        )
        EIS_PRODUCT_PART_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_PRODUCT_PART/EIS_PRODUCT_PART_WH.py",
            conn_id='spark_conn',
            task_id='EIS_PRODUCT_PART_WH',
            conf=config,
            dag=dag
        )
        EIS_ROUTING_OPERATION_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_ROUTING_OPERATION/EIS_ROUTING_OPERATION_WH.py",
            conn_id='spark_conn',
            task_id='EIS_ROUTING_OPERATION_WH',
            conf=config,
            dag=dag
        )
        EIS_WIP_FA_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA/rerun/EIS_WIP_FA_WH_rerun.py",
            conn_id='spark_conn',
            task_id='EIS_WIP_FA_WH',
            conf=config,
            dag=dag
        )
        EIS_WIP_FA_RUNCARD_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA_RUNCARD/EIS_WIP_FA_RUNCARD_WH.py",
            conn_id='spark_conn',
            task_id='EIS_WIP_FA_RUNCARD_WH',
            conf=config,
            dag=dag
        )
        EIS_WIP_FA_START_OPERATION_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA_START_OPERATION/EIS_WIP_FA_START_OPERATION_WH.py",
            conn_id='spark_conn',
            task_id='EIS_WIP_FA_START_OPERATION_WH',
            conf=config,
            dag=dag
        )
        EIS_WIP_FA_WIP_TRANS_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA_WIP_TRANS/rerun/EIS_WIP_FA_WIP_TRANS_WH_rerun.py",
            conn_id='spark_conn',
            task_id='EIS_WIP_FA_WIP_TRANS_WH',
            conf=config,
            dag=dag
        )
        EIS_WORK_WEEK_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WORK_WEEK/EIS_WORK_WEEK_WH.py",
            conn_id='spark_conn',
            task_id='EIS_WORK_WEEK_WH',
            conf=config,
            dag=dag
        )
    with TaskGroup("STAGING_WN_GROUP") as STAGING_WN_GROUP:
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
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA/rerun/EIS_WIP_FA_WN_rerun.py",
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
            application=f"s3a://notebooks/lakehouse/staging/EIS/EIS_WIP_FA_WIP_TRANS/rerun/EIS_WIP_FA_WIP_TRANS_WN_rerun.py",
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
    with TaskGroup("DIMENSION_DAGS_GROUPS_WA_1") as DIMENSION_DAGS_GROUPS_WA_1:
        MFG_LINE_DIM_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_LINE_DIM/MFG_LINE_DIM_WA.py",
            conn_id='spark_conn',
            task_id='MFG_LINE_DIM_WA',
            conf=config,
            dag=dag
        )
        MFG_MACHINE_DIM_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_MACHINE_DIM/MFG_MACHINE_DIM_WA.py",
            conn_id='spark_conn',
            task_id='MFG_MACHINE_DIM_WA',
            conf=config,
            dag=dag
        )
        MFG_OPERATION_DIM_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_OPERATION_DIM/MFG_OPERATION_DIM_WA.py",
            conn_id='spark_conn',
            task_id='MFG_OPERATION_DIM_WA',
            conf=config,
            dag=dag
        )
        MFG_PRODUCT_DIM_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_PRODUCT_DIM/initial/MFG_PRODUCT_DIM_WA.py",
            conn_id='spark_conn',
            task_id='MFG_PRODUCT_DIM_WA',
            conf=config,
            dag=dag
        )
    with TaskGroup("DIMENSION_DAGS_GROUPS_WA_2") as DIMENSION_DAGS_GROUPS_WA_2:
        MFG_ROUTING_OPERATION_DIM_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_ROUTING_OPERATION_DIM/MFG_ROUTING_OPERATION_DIM_WA.py",
            conn_id='spark_conn',
            task_id='MFG_ROUTING_OPERATION_DIM_WA',
            conf=config,
            dag=dag
        )
        SFC_RUNCARD_DIM_WA = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/SFC_RUNCARD_DIM/rerun/SFC_RUNCARD_DIM_WA_rerun.py",
            conn_id='spark_conn',
            task_id='SFC_RUNCARD_DIM_WA',
            conf=config,
            dag=dag
        )
    with TaskGroup("DIMENSION_DAGS_GROUPS_WH_1") as DIMENSION_DAGS_GROUPS_WH_1:
        MFG_LINE_DIM_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_LINE_DIM/MFG_LINE_DIM_WH.py",
            conn_id='spark_conn',
            task_id='MFG_LINE_DIM_WH',
            conf=config,
            dag=dag
        )
        MFG_MACHINE_DIM_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_MACHINE_DIM/MFG_MACHINE_DIM_WH.py",
            conn_id='spark_conn',
            task_id='MFG_MACHINE_DIM_WH',
            conf=config,
            dag=dag
        )
        MFG_OPERATION_DIM_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_OPERATION_DIM/MFG_OPERATION_DIM_WH.py",
            conn_id='spark_conn',
            task_id='MFG_OPERATION_DIM_WH',
            conf=config,
            dag=dag
        )
        MFG_PRODUCT_DIM_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_PRODUCT_DIM/initial/MFG_PRODUCT_DIM_WH.py",
            conn_id='spark_conn',
            task_id='MFG_PRODUCT_DIM_WH',
            conf=config,
            dag=dag
        )
    with TaskGroup("DIMENSION_DAGS_GROUPS_WH_2") as DIMENSION_DAGS_GROUPS_WH_2:
        MFG_ROUTING_OPERATION_DIM_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_ROUTING_OPERATION_DIM/MFG_ROUTING_OPERATION_DIM_WH.py",
            conn_id='spark_conn',
            task_id='MFG_ROUTING_OPERATION_DIM_WH',
            conf=config,
            dag=dag
        )
        SFC_RUNCARD_DIM_WH = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/SFC_RUNCARD_DIM/rerun/SFC_RUNCARD_DIM_WH_rerun.py",
            conn_id='spark_conn',
            task_id='SFC_RUNCARD_DIM_WH',
            conf=config,
            dag=dag
        )
    with TaskGroup("DIMENSION_DAGS_GROUPS_WN_1") as DIMENSION_DAGS_GROUPS_WN_1:
        MFG_DATE_DIM_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_DATE_DIM/MFG_DATE_DIM.py",
            conn_id='spark_conn',
            task_id='MFG_DATE_DIM_WN',
            conf=config,
            dag=dag
        )
        MFG_LINE_DIM_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_LINE_DIM/MFG_LINE_DIM_WN.py",
            conn_id='spark_conn',
            task_id='MFG_LINE_DIM_WN',
            conf=config,
            dag=dag
        )
        MFG_MACHINE_DIM_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_MACHINE_DIM/MFG_MACHINE_DIM_WN.py",
            conn_id='spark_conn',
            task_id='MFG_MACHINE_DIM_WN',
            conf=config,
            dag=dag
        )
        MFG_OPERATION_DIM_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_OPERATION_DIM/MFG_OPERATION_DIM_WN.py",
            conn_id='spark_conn',
            task_id='MFG_OPERATION_DIM_WN',
            conf=config,
            dag=dag
        )
        MFG_PRODUCT_DIM_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_PRODUCT_DIM/initial/MFG_PRODUCT_DIM_WN.py",
            conn_id='spark_conn',
            task_id='MFG_PRODUCT_DIM_WN',
            conf=config,
            dag=dag
        )
    with TaskGroup("DIMENSION_DAGS_GROUPS_WN_2") as DIMENSION_DAGS_GROUPS_WN_2:
        MFG_ROUTING_OPERATION_DIM_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_ROUTING_OPERATION_DIM/MFG_ROUTING_OPERATION_DIM_WN.py",
            conn_id='spark_conn',
            task_id='MFG_ROUTING_OPERATION_DIM_WN',
            conf=config,
            dag=dag
        )
        SFC_RUNCARD_DIM_WN = EzSparkSubmitOperator(
            spark_binary='spark-submit',
            application=f"s3a://notebooks/lakehouse/master/DIM/SFC_RUNCARD_DIM/rerun/SFC_RUNCARD_DIM_WN_rerun.py",
            conn_id='spark_conn',
            task_id='SFC_RUNCARD_DIM_WN',
            conf=config,
            dag=dag
        )
    SFC_WIP_RUNCARD_FACT_WA = EzSparkSubmitOperator(
        spark_binary='spark-submit',
        application=f"s3a://notebooks/lakehouse/process/FACT/SFC_WIP_RUNCARD_FACT/rerun/SFC_WIP_RUNCARD_FACT_WA_rerun.py",
        conn_id='spark_conn',
        task_id='SFC_WIP_RUNCARD_FACT_WA',
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
    SFC_WIP_RUNCARD_FACT_WH = EzSparkSubmitOperator(
        spark_binary='spark-submit',
        application=f"s3a://notebooks/lakehouse/process/FACT/SFC_WIP_RUNCARD_FACT/rerun/SFC_WIP_RUNCARD_FACT_WH_rerun.py",
        conn_id='spark_conn',
        task_id='SFC_WIP_RUNCARD_FACT_WH',
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
    SFC_WIP_RUNCARD_FACT_WN = EzSparkSubmitOperator(
        spark_binary='spark-submit',
        application=f"s3a://notebooks/lakehouse/process/FACT/SFC_WIP_RUNCARD_FACT/rerun/SFC_WIP_RUNCARD_FACT_WN_rerun.py",
        conn_id='spark_conn',
        task_id='SFC_WIP_RUNCARD_FACT_WN',
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
    CUMULATIVE_YIELD = EzSparkSubmitOperator(
        spark_binary='spark-submit',
        application=f"s3a://notebooks/lakehouse/mart/CUMULATIVE_YIELD.py",
        conn_id='spark_conn',
        task_id='CUMULATIVE_YIELD',
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

    task1 >> [STAGING_WN_GROUP,STAGING_WH_GROUP,STAGING_WA_GROUP]
    STAGING_WN_GROUP >> DIMENSION_DAGS_GROUPS_WN_1 >> DIMENSION_DAGS_GROUPS_WN_2
    STAGING_WH_GROUP >> DIMENSION_DAGS_GROUPS_WH_1 >> DIMENSION_DAGS_GROUPS_WH_2
    STAGING_WA_GROUP >> DIMENSION_DAGS_GROUPS_WA_1 >> DIMENSION_DAGS_GROUPS_WA_2
    [DIMENSION_DAGS_GROUPS_WN_2,DIMENSION_DAGS_GROUPS_WH_2,DIMENSION_DAGS_GROUPS_WA_2] >> SFC_WIP_RUNCARD_FACT_WN >> [SFC_WIP_RUNCARD_FACT_WH,SFC_WIP_RUNCARD_FACT_WA] >> CUMULATIVE_YIELD
