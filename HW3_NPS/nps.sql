-- creating a database
create or replace database nps;

-- creating 2 schemas 
create or replace schema raw_data;
create or replace schema analytics;

use schema raw_data;

-- Create NPS table with primary key
create or replace table nps(
    id integer primary key,
    created timestamp,
    score integer);

-- checking tables
select * from nps;

-- creating external stage
create or replace stage nps_stage
url = 's3://s3-geospatial/readonly/nps.csv'
file_format = (type = csv);

list @nps_stage;

-- copying data into table
copy into nps 
from @nps_stage
file_format = (type = 'csv', skip_header=1);

--cleaning data
select * from nps where id is null or created is null or score is null;

select id, count(*) from nps
group by 1 having count(*) > 1;

select * from nps where score > 10 or score < 0;
-- query to calculate monthly NPS
select left(created, 7) as month, round((sum(case when score >=9 then 1 when score <=6 then -1 else 0 end)) / count(*) * 100, 1) as average_nps from nps
group by 1
order by 1;

-- ctas query
create or replace table analytics.nps_summary as select 
left(created, 7) as month, round((sum(case when score >=9 then 1 when score <=6 then -1 else 0 end)) / count(*) * 100, 1) as average_nps from raw_data.nps
group by 1
order by 1;

-- nps_summary table
select * from analytics.nps_summary;





select left(created, 7) as month, round((sum(case when score >=9 then 1 when score <=6 then -1 else 0 end)) / count(*) * 100, 1) as average_nps from nps
group by 1
order by 1;
