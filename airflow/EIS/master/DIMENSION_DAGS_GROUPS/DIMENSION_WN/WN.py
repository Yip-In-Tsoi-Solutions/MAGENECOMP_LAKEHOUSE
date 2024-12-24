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
    dag_id="DIMENSION_WN",
    default_args=default_args,
    description='Run DAG everyday at 7:00',
    schedule_interval='0 7 * * *',
    catchup=False,
) as dag:
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
            application=f"s3a://notebooks/lakehouse/master/DIM/MFG_PRODUCT_DIM/MFG_PRODUCT_DIM_WN.py",
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
            application=f"s3a://notebooks/lakehouse/master/DIM/SFC_RUNCARD_DIM/SFC_RUNCARD_DIM_WN.py",
            conn_id='spark_conn',
            task_id='SFC_RUNCARD_DIM_WN',
            conf=config,
            dag=dag
        )
        DIMENSION_DAGS_GROUPS_WN_1 >> DIMENSION_DAGS_GROUPS_WN_2