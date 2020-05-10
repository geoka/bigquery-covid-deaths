-- Mobility trends and deaths announced
with mob as (
SELECT 
country_region ,country_region_code, date,

round(avg(residential_percent_change_from_baseline),0) as residential_percent_change_from_baseline
,round(avg(retail_and_recreation_percent_change_from_baseline ),0) as retail_and_recreation_percent_change_from_baseline
,round(avg(grocery_and_pharmacy_percent_change_from_baseline) ,0) as grocery_and_pharmacy_percent_change_from_baseline
,round(avg(parks_percent_change_from_baseline) ,0) as parks_percent_change_from_baseline
,round(avg(transit_stations_percent_change_from_baseline) ,0) as transit_stations_percent_change_from_baseline
,round(avg(workplaces_percent_change_from_baseline) ,0) as workplaces_percent_change_from_baseline

FROM `bigquery-public-data.covid19_google_mobility.mobility_report` 
-- where country_region = 'United Kingdom'
group by 1,2,3
order by 3 desc
)

-- select * from mob;

, death_tmp as (
select 
country_region
,date
,avg(latitude) as lat
,avg(longitude) as lon
,sum(deaths) as deaths_total

from `bigquery-public-data.covid19_jhu_csse.summary`
group by 1,2
order by 1,2 desc
)

-- add the daily death count
, deaths_base as (select 
*
,deaths_total - lag(deaths_total,1) over (partition by country_region order by date asc) as deaths_daily
from death_tmp)

-- select * from deaths_country;

-- find date with max daily deaths per country
,deaths_max_country_date as (
select 
country_region
,date as date_max_daily_deaths
, deaths_daily as deaths_daily_max
from  (select 
  *
  , row_number() over(partition by country_region order by deaths_daily desc) as rn
  from deaths_base )
where rn = 1
)
-- find date with Nth cumulative death per country
,Nth_death_country_date as (
select 
  country_region,date,deaths_total
  ,sum(case when deaths_total >= 10 then 1 else 0 end) over(partition by country_region order by date asc rows between unbounded preceding and current row) as days_since_10th_death
  from deaths_base 

order by 1,2 desc
)
-- select * from Nth_death_country_date;

-- join it with base
,joined as (
select 
d1.*
,d2.date_max_daily_deaths
,d2.deaths_daily_max
,d3.days_since_10th_death

from deaths_base d1
join deaths_max_country_date d2
  on d1.country_region = d2.country_region
join Nth_death_country_date d3
  on d1.country_region = d3.country_region
  and d1.date = d3.date
)

-- select * from joined;

select 
d.*
,m.* except (country_region, date)
from joined d
left join mob m
  on d.country_region = m.country_region 
  and d.date = m.date
order by country_region desc,date desc
