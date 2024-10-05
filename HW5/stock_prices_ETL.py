#importing all the libraries for airflow and python
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import requests
import snowflake.connector

#function to create snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

#extract task to extract data from the API key using variable.get
@task
def extract_stock_data():
    api_key = Variable.get("alpha_vantage_api_key")
    stock_symbol = "TTWO"  # Take Two Interactive stock symbol
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={api_key}'
    
    response = requests.get(url)
    data = response.json()
    return data["Time Series (Daily)"]

#to transform the data in desired table structure
@task
def transform_stock_data(raw_data):
    stock_symbol = "TTWO"
    transformed_data = [] #empty list
    for date, price_info in raw_data.items():
        transformed_data.append({
            'date': date,
            'symbol': stock_symbol,
            'open': price_info['1. open'],
            'high': price_info['2. high'],
            'low': price_info['3. low'],
            'close': price_info['4. close'],
            'volume': price_info['5. volume']
        })
    return transformed_data[:90]  # Only take the last 90 days

#task to load the transformed data in snowflake
@task
def load_to_snowflake(cur, data):
    try:
        cur.execute("BEGIN;")
        cur.execute("USE WAREHOUSE XSMALL;")
        cur.execute("CREATE OR REPLACE DATABASE stock;")
        cur.execute("USE DATABASE stock;")
        cur.execute("CREATE OR REPLACE SCHEMA raw_data;")
        cur.execute("USE SCHEMA raw_data;")
        
        # creating table
        cur.execute("""
            CREATE OR REPLACE TABLE stock.raw_data.stock_prices (
                date DATE PRIMARY KEY,
                symbol STRING,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT
            );
        """)
        
        for record in data:
            sql = f"""
                INSERT INTO stock.raw_data.stock_prices (date, symbol, open, high, low, close, volume)
                VALUES ('{record['date']}', '{record['symbol']}', {record['open']}, {record['high']}, {record['low']}, {record['close']}, {record['volume']});
            """
            cur.execute(sql)
        
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e

with DAG(
    dag_id='stock_etl',
    start_date=datetime(2024, 10, 3),
    schedule_interval='@daily',
    catchup=False,
    tags=['stock_prices', 'ETL']
) as dag:

    cur = return_snowflake_conn()
    raw_data = extract_stock_data()
    transformed_data = transform_stock_data(raw_data)
    load_to_snowflake(cur, transformed_data)
