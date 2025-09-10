from airflow import DAG
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "daily_csv_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["hdfs", "spark", "elasticsearch"],
) as dag:

    wait_for_hdfs_file = HdfsSensor(
        task_id="wait_for_hdfs_file",
        filepath="/data/raw/csv/daily_{{ ds }}.csv",
        poke_interval=60,
        timeout=3600,
        hdfs_conn_id="hdfs_default",
    )

    process_csv = SparkSubmitOperator(
        task_id="process_csv",
        application="/opt/spark/app/process_csv.py",
        conn_id="spark_default",
        application_args=[],
        packages="org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0"  # required for later ES writes too
    )

    index_to_es = SparkSubmitOperator(
        task_id="index_to_elasticsearch",
        application="/opt/spark/app/index_to_es.py",
        conn_id="spark_default",
        packages="org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0"
    )

    wait_for_hdfs_file >> process_csv >> index_to_es
