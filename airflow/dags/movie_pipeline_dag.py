from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Cấu hình mặc định cho kịch bản
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Khởi tạo DAG 
with DAG(
    'movie_data_pipeline',
    default_args=default_args,
    description='Pipeline chạy Spark Batch bằng BashOperator',
    schedule=timedelta(days=1), # <-- Đã sửa thành 'schedule' chuẩn Airflow 3
    start_date=datetime(2026, 9, 24),
    catchup=False,
    tags=['bigdata_project'],
) as dag:

    # Gọi file Spark
    run_spark_job = BashOperator(
        task_id='run_spark_processing',
        bash_command='python3 /home/dell/process_movies.py'
    )

    run_spark_job
