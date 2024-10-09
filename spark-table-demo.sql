-- Databricks notebook source


-- COMMAND ----------

DROP TABLE IF EXISTS demo_db.fire_service_calls_tbl;
drop view if exists demo_db;

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/demo_db.db/
-- MAGIC

-- COMMAND ----------

create database if not exists demo_db

-- COMMAND ----------

create table if not exists demo_db.fire_service_calls_tbl(
  CallNumber integer,
  UnitID string,
  IncidentNumber integer,
  CallType string,
  CallDate string,
  WatchDate string,
  CallFinalDisposition string,
  AvailableDtTm string,
  Address string,
  City string,
  Zipcode integer,
  Battalion string,
  StationArea string,
  Box string,
  OriginalPriority string,
  Priority string,
  FinalPriority integer,
  ALSUnit boolean,
  CallTypeGroup string,
  NumAlarms integer,
  UnitType string,
  UnitSequenceInCallDispatch integer,
  FirePreventionDistrict string,
  SupervisorDistrict string,
  Neighborhood string,
  Location string,
  RowID string,
  Delay float
) using parquet

-- COMMAND ----------

DROP VIEW IF EXISTS global_temp.fire_service_calls_view;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC raw_fire_df = spark.read \
-- MAGIC             .format("csv") \
-- MAGIC             .option("header", "true") \
-- MAGIC             .option("inferSchema", "true") \
-- MAGIC             .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC raw_fire_df.createGlobalTempView("fire_service_calls_view")

-- COMMAND ----------

select * from global_temp.fire_service_calls_view

-- COMMAND ----------

truncate table demo_db.fire_service_calls_tbl

-- COMMAND ----------

insert into demo_db.fire_service_calls_tbl
select * from global_temp.fire_service_calls_view

-- COMMAND ----------

select * from demo_db.fire_service_calls_tbl limit 10

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Q1. How many distinct types of calls were made to the Fire Department?

-- COMMAND ----------

select count(distinct(callType)) as distinct_call_type_count
from demo_db.fire_service_calls_tbl
where callType is not null;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC #### Q2. What were distinct types of calls made to the Fire Department?

-- COMMAND ----------

select distinct callType as distinct_call
from demo_db.fire_service_calls_tbl
where callType is not null


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Q3. Find out all reponses for delayed times greater than 5 mins

-- COMMAND ----------

select callNumber, Delay
from demo_db.fire_service_calls_tbl
where Delay > 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Q4. what were the most common call types?

-- COMMAND ----------

select callType, count(*) as count
from demo_db.fire_service_calls_tbl
where callType is not null
group by CallType
order by count desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q5. What zip codes accounted for most common calls?

-- COMMAND ----------

select callType, Zipcode, count(*) as count
from demo_db.fire_service_calls_tbl
where callType is not null
group by callType, Zipcode
order by count desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

-- COMMAND ----------

select distinct Zipcode,  Neighborhood
from demo_db.fire_service_calls_tbl
where Zipcode == 94102 or Zipcode == 94103

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Q7. What was the sum of all call alarms, average, min, and max of the call response times?

-- COMMAND ----------

select sum(NumAlarms) as sum_alarms, avg(Delay) as avg_delay, min(Delay) as min_delay, max(Delay) as max_delay
from demo_db.fire_service_calls_tbl

-- COMMAND ----------

select * from demo_db.fire_service_calls_tbl limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q8. How many distinct years of data is in the data set?

-- COMMAND ----------

select distinct year(to_date(callDate, 'yyyy-MM-dd')) as year_num
from demo_db.fire_service_calls_tbl
order by year_num 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q9. What week of the year in 2018 had the most fire calls?

-- COMMAND ----------

select weekofyear(to_date(callDate, 'yyyy-MM-dd')) as week_year, count(*) as count
from demo_db.fire_service_calls_tbl
where year(to_date(callDate, 'yyyy-MM-dd')) == 2018
group by week_year
order by count desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q10. What neighborhoods in San Francisco had the worst response time in 2018?
-- MAGIC

-- COMMAND ----------

select Neighborhood, Delay
from demo_db.fire_service_calls_tbl
where year(to_date(callDate, 'yyyy-MM-dd')) == 2018
order by Delay DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
