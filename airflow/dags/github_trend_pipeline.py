from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

# scripts 폴더 import 경로 추가
sys.path.insert(0, '/home/airflow/gcs/dags/scripts')

from extract_github import extract_github_data
from upload_to_gcs import upload_to_gcs
from create_external_table import create_external_table

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 4),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'github_trend_pipeline',
    default_args=default_args,
    description='GitHub 트렌드 분석 파이프라인',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['github', 'data-engineering'],
) as dag:

    # Task 1: GitHub 데이터 수집
    extract = PythonOperator(
        task_id='extract_github_data',
        python_callable=extract_github_data,
        provide_context=True,
    )

    # Task 2: GCS에 업로드
    upload = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        provide_context=True,
    )

    # Task 3: BigQuery External Table 생성
    create_table = PythonOperator(
        task_id='create_external_table',
        python_callable=create_external_table,
    )

    # Task 4: dbt run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /home/airflow/gcs/dags/dbt && dbt run --profiles-dir .',
    )

    # Task 5: dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /home/airflow/gcs/dags/dbt && dbt test --profiles-dir .',
    )

    # 실행 순서
    extract >> upload >> create_table >> dbt_run >> dbt_test