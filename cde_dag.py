from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
# 수정된 Operator Import
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

default_args = {
    'owner': 'cde_user',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='cde_spark_integration_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['cde', 'spark', 'git_test'],
) as dag:

    start_task = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "=== Airflow 파이프라인 시작 ==="'
    )
    # 수정된 Task 부분
    run_spark_job = CDEJobRunOperator(
        task_id='run_spark_test',
        job_name='cde-spark-test-job'  # 실행할 CDE Spark Job 이름
    )

    end_task = BashOperator(
        task_id='end_pipeline',
        bash_command='echo "=== Airflow 파이프라인 완료 ==="'
    )

    start_task >> run_spark_job >> end_task
