from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime  # Importing datetime

# Function to return Snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# Task to join the tables and create the session_summary table
@task
def create_joined_table():
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        # Use the database and schema
        cur.execute("USE DATABASE dev1;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS dev1.analytics;")
        cur.execute("USE SCHEMA dev1.analytics;")
        
        # Create the session_summary table with two duplicate checks
        cur.execute("""
            CREATE OR REPLACE TABLE dev1.analytics.session_summary AS
            SELECT 
                u.userId,
                u.sessionId,
                u.channel,
                s.ts,
                ROW_NUMBER() OVER (PARTITION BY u.sessionId ORDER BY s.ts) AS row_num,
                COUNT(*) OVER (PARTITION BY u.userId, u.sessionId, u.channel) AS duplicate_count
            FROM dev1.raw_data.user_session_channel u
            JOIN dev1.raw_data.session_timestamp s
            ON u.sessionId = s.sessionId
            QUALIFY row_num = 1 AND duplicate_count = 1;  -- Two conditions for removing duplicates
        """)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error in creating joined table: {e}")
        raise e


# Define the DAG
with DAG(
    dag_id='ELTTableJoin',
    start_date=datetime(2024, 10, 17), 
    catchup=False,
    tags=['ELT', 'JOIN', 'Snowflake', 'Duplicates', 'WAU'],
    schedule_interval='30 3 * * *'
) as dag:

    join_task = create_joined_table()

    join_task
