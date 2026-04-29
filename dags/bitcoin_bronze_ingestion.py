from datetime import datetime
import requests
import json

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="bitcoin_bronze_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["crypto", "bronze"],
) as dag:

    @task
    def extract():
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "brl",
            "ids": "bitcoin"
        }

        response = requests.get(url, params=params)
        data = response.json()[0]  # retorna lista

        return data

    @task
    def transform(data: dict):
        return {
            "price_brl": data["current_price"],
            "market_cap": data.get("market_cap"),
            "volume_24h": data.get("total_volume"),
            "source": "coingecko",
            "collected_at": datetime.utcnow().isoformat(),
            "raw_json": json.dumps(data)
        }

    @task
    def load(record: dict):
        hook = PostgresHook(postgres_conn_id="postgres_local")

        sql = """
        INSERT INTO bitcoin.price (
            price_brl,
            market_cap,
            volume_24h,
            source,
            collected_at,
            raw_json
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        hook.run(
            sql,
            parameters=(
                record["price_brl"],
                record["market_cap"],
                record["volume_24h"],
                record["source"],
                record["collected_at"],
                record["raw_json"],
            ),
        )

    # pipeline
    data = extract()
    record = transform(data)
    load(record)