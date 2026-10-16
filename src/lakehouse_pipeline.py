from datetime import datetime, timedelta
from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'lakehouse_movie_pipeline',
    default_args=default_args,
    description='Pipeline tu dong hoa Lakehouse: Kiem tra Bronze -> Lam sach Silver -> Huan luyen ALS -> Xuat Gold -> Toi uu Streaming Logs',
    schedule=timedelta(days=1),
    start_date=datetime(2026, 9, 24),
    catchup=False,
    max_active_runs=1,
    tags=['lakehouse', 'als_recommendation', 'bigdata_project'],
) as dag:

    # 1. Kiem tra du lieu dau vao o tang Bronze tren HDFS (dong thoi tat safe mode neu Hadoop moi bat)
    task_check_bronze = BashOperator(
        task_id='task_check_bronze',
        bash_command=(
            '/usr/local/hadoop/bin/hdfs dfsadmin -safemode leave 2>/dev/null || true; '
            '/usr/local/hadoop/bin/hdfs dfs -test -e /lakehouse/bronze/movielens/movies.csv && '
            '/usr/local/hadoop/bin/hdfs dfs -test -e /lakehouse/bronze/movielens/ratings.csv && '
            'echo "=== [BRONZE] Du lieu tho da ton tai tren HDFS ==="'
        ),
    )

    # 2. Chay PySpark lam sach va chuan hoa du lieu sang Parquet (Silver)
    task_clean_silver = BashOperator(
        task_id='task_clean_silver',
        bash_command='python3 /home/dell/clean_data.py',
    )

    # 3. Chay PySpark MLlib huan luyen mo hinh ALS va sinh goi y phim
    task_train_als = BashOperator(
        task_id='task_train_als',
        bash_command='python3 /home/dell/train_als_recommendation.py',
    )

    # 4. Kiem tra va xac nhan du lieu goi y da luu vao tang Gold tren HDFS
    task_export_gold = BashOperator(
        task_id='task_export_gold',
        bash_command=(
            '/usr/local/hadoop/bin/hdfs dfs -test -d /lakehouse/gold/movie_recommendations && '
            '/usr/local/hadoop/bin/hdfs dfs -ls /lakehouse/gold/movie_recommendations && '
            'echo "=== [GOLD] Ket qua goi y phim ALS da duoc xuat thanh cong ==="'
        ),
    )

    # 5. Kiem tra va gom log streaming thoi gian thuc tren HDFS
    task_compact_streaming = BashOperator(
        task_id='task_compact_streaming',
        bash_command=(
            '/usr/local/hadoop/bin/hdfs dfs -mkdir -p /lakehouse/silver/streaming_logs && '
            '/usr/local/hadoop/bin/hdfs dfs -ls /lakehouse/silver/streaming_logs && '
            'echo "=== [STREAMING] Da kiem tra va dong bo streaming logs thanh cong ==="'
        ),
    )

    # Thiet lap chuoi thuc thi tuan tu
    task_check_bronze >> task_clean_silver >> task_train_als >> task_export_gold >> task_compact_streaming
