from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime


@dag(
    dag_id="bitcoin_spark_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def spark_pipeline():

    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        spark-submit \
          --master spark://192.168.1.23:7077 \
          --deploy-mode cluster \
          /usr/local/airflow/include/spark_jobs/bitcoin_job.py
        """
    )

    run_spark


dag = spark_pipeline()