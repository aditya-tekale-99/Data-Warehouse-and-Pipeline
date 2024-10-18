# ETL/ELT Snowflake

## Project Overview
This project contains an Airflow DAG that performs an ETL and ELT operation to create tables in Snowflake, load data from an S3 bucket and join two tables to create a new table. The DAG automates the creation of tables in Snowflake, followed by the loading of data from CSV files stored in S3 into these tables. The DAG is scheduled to run daily.


## Tech Stack
- **Airflow**: Orchestrates the ETL process.
- **Snowflake**: Cloud data warehouse where the data is loaded.
- **S3**: The data source, storing CSV files.
- **Python**: Used to script the DAG and handle connections.

## [ETL.py](https://github.com/aditya-tekale-99/Data-Warehouse-and-Pipeline/blob/main/HW7/ETL.py) DAG Explanation
- **DAG Name**: `ETLTable`
- **Tasks**:
   1. **load()**:
      - Creates the `dev1` database and `raw_data` schema in Snowflake if they do not already exist.
      - Creates two tables: 
        - `user_session_channel`: Stores session and channel data.
        - `session_timestamp`: Stores session timestamps.
   2. **load_data()**:
      - Creates a stage in Snowflake to connect to the S3 bucket.
      - Loads the CSV data from S3 into the two Snowflake tables (`user_session_channel` and `session_timestamp`).
   - The tasks are executed in sequence, with `load()` creating the tables first, followed by `load_data()` to load data into them.

 ## [ELT.py](https://github.com/aditya-tekale-99/Data-Warehouse-and-Pipeline/blob/main/HW7/ELT.py) DAG Explanation
- **DAG Name**: `ELTTableJoin`
- **Task**: The task `create_joined_table` connects to Snowflake and performs the following:
   1. Ensures that the `dev1.analytics` schema exists.
   2. Joins `user_session_channel` and `session_timestamp` tables.
   3. Removes duplicates by applying **two distinct checks**:
      - **`ROW_NUMBER()`**: Ensures only the first occurrence of each `sessionId` (based on the timestamp `s.ts`) is selected.
      - **`COUNT(*) OVER (PARTITION BY u.userId, u.sessionId, u.channel)`**: Counts the total occurrences for each combination of `userId`, `sessionId`, and `channel`. Only records with a count of 1 are retained.
   4. Creates a `session_summary` table in Snowflake with the cleaned data.
- **Duplicate Handling**: The query applies two conditions to ensure no duplicate records are included in the `session_summary` table:
   1. Retains only the first occurrence of each `sessionId` by using `ROW_NUMBER()`.
   2. Ensures there is only one occurrence of each `userId`, `sessionId`, and `channel` combination using `COUNT(*)`.

## Error Handling
In case of an error during the table creation or data load, the transaction is rolled back to avoid partial or incorrect data being committed to Snowflake. Error messages are printed, and the DAG task will fail, which can be monitored via the Airflow UI.
