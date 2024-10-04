# data226
Projects done in Data warehouse and pipeline

### [stock_prices_ETL.py](https://github.com/aditya-tekale-99/data226/blob/main/HW5/stock_prices_ETL.py)
**Description**:

This Airflow DAG performs an ETL (Extract, Transform, Load) process for daily stock price data from Alpha Vantage to Snowflake. The DAG is designed to run daily, retrieving the latest stock prices for Take-Two Interactive (stock symbol: TTWO), transforming the data, and loading it into a Snowflake table for further analysis.

**Steps:**
1. Extract: The extract_stock_data() task fetches the daily stock prices for TTWO using Alpha Vantage's API. The API key is securely stored as an Airflow Variable (alpha_vantage_api_key).
   
2. Transform: The transform_stock_data(raw_data) task processes the raw API data, extracting relevant fields (open, high, low, close, volume) and limiting the dataset to the last 90 days.

3. Load: The load_to_snowflake(cur, data) task loads the transformed data into a Snowflake table (raw_data.stock_prices). This task uses a Snowflake connection (snowflake_conn) via the SnowflakeHook. The table is created (or replaced) in Snowflake if it doesn't already exist, and the daily data is inserted. In case of an error, the task will rollback the transaction.

**DAG Configuration:**
- The DAG is scheduled to run daily (@daily).
- It begins execution from September 30, 2024 (start_date=datetime(2024, 9, 30)).
- No backfill or catchup (catchup=False).
- Tagged with stock_prices and ETL for better organization.

