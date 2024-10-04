from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import requests
import snowflake.connector

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract_stock_data():
    api_key = Variable.get("alpha_vantage_api_key")
    stock_symbol = "TTWO"  # Take Two Interactive stock symbol
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={api_key}'
    
    response = requests.get(url)
    data = response.json()
    return data["Time Series (Daily)"]

@task
def transform_stock_data(raw_data):
    transformed_data = []
    for date, price_info in raw_data.items():
        transformed_data.append({
            'date': date,
            'open': price_info['1. open'],
            'high': price_info['2. high'],
            'low': price_info['3. low'],
            'close': price_info['4. close'],
            'volume': price_info['5. volume']
        })
    return transformed_data[:90]  # Only take the last 90 days

@task
def load_to_snowflake(cur, data):
    try:
        cur.execute("BEGIN;")
        cur.execute("""
            CREATE OR REPLACE TABLE raw_data.stock_prices (
                date DATE PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT
            );
        """)
        
        for record in data:
            sql = f"""
                INSERT INTO raw_data.stock_prices (date, open, high, low, close, volume)
                VALUES ('{record['date']}', {record['open']}, {record['high']}, {record['low']}, {record['close']}, {record['volume']});
            """
            cur.execute(sql)
        
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e

with DAG(
    dag_id='stock_price_etl',
    start_date=datetime(2024, 9, 30),
    schedule_interval='@daily',
    catchup=False,
    tags=['stock_prices', 'ETL']
) as dag:

    cur = return_snowflake_conn()
    raw_data = extract_stock_data()
    transformed_data = transform_stock_data(raw_data)
    load_to_snowflake(cur, transformed_data)
