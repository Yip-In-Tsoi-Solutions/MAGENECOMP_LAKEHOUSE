from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from airflow.models import Variable


confirm_initial_flag = Variable.get('confirm_initial_flag')
# confirm_initial_flag = False

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
    send_email(to=["peerapon.li@yipintsoi.com,korakot.pu@yipintsoi.com,Chirapan.Ph@yipintsoi.com"], subject=subject, html_content=html_content)
    # send_email(to=["korakot.pu@yipintsoi.com"], subject=subject, html_content=html_content)

# Set up default arguments with the on_failure_callback
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,  # Set this to False because we're using a custom alert
    'on_failure_callback': failure_email_alert,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define a sample task that intentionally fails to trigger the alert
def failing_task():
    print(f"Debug: confirm_initial_flag = {confirm_initial_flag}")  # Print debug information
    if confirm_initial_flag == 'T':
        print("Process skipped.")
        #อัปเดตค่า Variable confirm_initial_flag = 'F' เพื่อต้องการให้ manual เปลี่ยนเป็น 'T' ทุกครั้งที่จะรัน
        Variable.set('confirm_initial_flag', 'F')
    else:
        raise Exception("Intentional error for testing custom email alert.")


def end():
     print('end')

# Define the DAG with the custom email alert callback
with DAG(
    dag_id='custom_dag_with_email_alert',
    default_args=default_args,
    description='DAG with custom email alert on failure',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 11, 6),
    catchup=False,
) as dag:
    # Define task
    task1 = PythonOperator(
        task_id='failing_task',
        python_callable=failing_task
    )
    task2 = PythonOperator(
        task_id='end',
        python_callable=end
    )

task1 >> task2

