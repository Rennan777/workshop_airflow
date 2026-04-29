from datetime import datetime
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="bitcoin_price_taskflow",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",  # ✅ novo padrão
    catchup=False,
    tags=["crypto", "taskflow"],
) as dag:

    @task
    def extract_bitcoin_price():
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "bitcoin",
            "vs_currencies": "brl"
        }

        response = requests.get(url, params=params)
        data = response.json()

        return data["bitcoin"]["brl"]

    @task
    def load_to_postgres(price: float):
        hook = PostgresHook(postgres_conn_id="postgres_local")

        hook.run(
            """
            INSERT INTO teste01.bitcoin_price (price_brl)
            VALUES (%s)
            """,
            parameters=(price,),
        )

    price = extract_bitcoin_price()
    load_to_postgres(price)