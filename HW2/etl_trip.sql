-- Create Database 'trip_db', Schema 'trip_schema' and Table 'trip_data'
create or replace database trip_db;

create or replace schema trip_schema;

create or replace table trip_data(
trip_duration integer,
start_time timestamp,
stop_time timestamp,
start_station_id integer,
start_station_name string,
start_station_lat float,
start_station_lon float,
end_station_id integer,
end_station_name string,
end_station_lat float,
end_station_lon float,
id integer,
membership string,
user_type string,
birth_year integer,
gender integer);

-- checking the tables 
select * from trip_data limit 5;

-- Create Stage 'trip_stage'
create or replace stage trip_stage
url = 's3://snowflake-workshop-lab/citibike-trips-csv'
file_format = (type = csv);

-- list stage
list @trip_stage;

-- Create File Format 'trip_file_format'
create or replace file format trip_file_format type='csv'
compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
date_format = 'auto' timestamp_format = 'auto' null_if = ('');

-- load data into table
copy into trip_data
from @trip_stage
file_format = (format_name = 'trip_file_format')
force = true;

-- Write an SQL query to produce a report that shows, for each hour of the day, the following:
-- The total number of trips that started during that hour.
-- The average duration of these trips in mins.
-- The average distance traveled during these trips in kms.
select to_char(start_time, 'HH24') as hour, count(*) as total_count_of_trips, avg(trip_duration/60) as avg_duration_mins, avg(haversine(start_station_lat, start_station_lon, end_station_lat,end_station_lon)) as avg_distance_kms
from trip_data
group by 1
order by 1;

-- create a new x large warehouse
create or replace warehouse xlarge warehouse_size = 'X-LARGE';

-- switch warehouses  
use warehouse compute_wh; -- xsmall
use warehouse xlarge;

-- drop contents of the table
delete from trip_data;
