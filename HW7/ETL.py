from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Function to return Snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# Task to create tables in Snowflake
@task
def load():
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute("CREATE DATABASE IF NOT EXISTS dev1;")
        cur.execute("USE DATABASE dev1;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw_data;")
        cur.execute("USE SCHEMA dev1.raw_data;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dev1.raw_data.user_session_channel (
                userId int not NULL,
                sessionId varchar(32) primary key,
                channel varchar(32) default 'direct'
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dev1.raw_data.session_timestamp (
                sessionId varchar(32) primary key,
                ts timestamp
            );
        """)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

# Task to create stage and load data into the tables
@task
def load_data():
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        # Create stage and load data from S3 into Snowflake tables
        cur.execute("""
            CREATE OR REPLACE STAGE dev1.raw_data.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """)
        cur.execute("""
            COPY INTO dev1.raw_data.user_session_channel
            FROM @dev1.raw_data.blob_stage/user_session_channel.csv;
        """)
        cur.execute("""
            COPY INTO dev1.raw_data.session_timestamp
            FROM @dev1.raw_data.blob_stage/session_timestamp.csv;
        """)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

# Define the DAG
with DAG(
    dag_id='ETLTable',
    start_date=datetime(2024, 10, 17),
    catchup=False,
    tags=['ETL', 'HW7', 'S3', 'WAU'],
    schedule_interval='30 2 * * *'
) as dag:
    
    # Set up task dependencies
    load_task = load()
    load_data_task = load_data()
    
    # task order
    load_task >> load_data_task
