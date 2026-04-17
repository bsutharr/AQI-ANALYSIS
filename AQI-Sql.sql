use aqi;

select * from aqi;
select count(*) from aqi;

# validating datatypes
describe aqi;

# Checking Null values for each columns
SELECT
    COUNT(*) AS total_rows,
    SUM(city IS NULL)        AS city_nulls,
    SUM(date IS NULL)        AS date_nulls,
    SUM(`pm2.5` IS NULL)       AS pm2_5_nulls,
    SUM(PM10 IS NULL)        AS pm10_nulls,
    SUM(NO IS NULL)          AS no_nulls,
    SUM(no2 IS NULL)         AS no2_nulls,
    SUM(NOx IS NULL)         AS nox_nulls,
    SUM(nh3 IS NULL)         AS nh3_nulls,
    SUM(co IS NULL)          AS co_nulls,
    SUM(so2 IS NULL)         AS so2_nulls,
    SUM(o3 IS NULL)          AS o3_nulls,
    SUM(benzene IS NULL)     AS benzene_nulls,
    SUM(toluene IS NULL)     AS toluene_nulls,
    SUM(xylene IS NULL)      AS xylene_nulls,
    SUM(aqi IS NULL)         AS aqi_nulls,
    SUM(aqi_bucket IS NULL)  AS aqi_bucket_nulls
FROM aqi;

# creating a copy or backup of raw data

create table raw_aqi as select * from aqi;
select * from raw_aqi;
select * from cleaned;

# deriving new table city month using cleaned table
CREATE table city_month_aqi AS
select
    City,
    DATE_FORMAT(Date, '%Y-%m') AS YearMonth,
    ROUND(AVG(AQI), 2) AS Monthly_AQI
FROM cleaned
GROUP BY City, DATE_FORMAT(Date, '%Y-%m');

-- Now Joining using JOIN
SELECT
    d.Date, d.City, d.AQI AS Daily_AQI, d.AQI_Bucket,m.Monthly_AQI
FROM cleaned d
JOIN city_month_aqi m
    ON d.City = m.City
   AND DATE_FORMAT(d.Date, '%Y-%m') = m.YearMonth;

# Using Subquery to find national avarge above cities
Select City, round(avg(AQI),2) as high_aqi
from cleaned 
group by City
having avg(AQI) > (select avg(aqi) from cleaned)
order by high_aqi desc;

# window function -- 	Rank cities by AQI within each year

select city, avg(aqi), year(date),
RANK() over (partition by year(date) order by avg(aqi) desc) as rnk_cities
from cleaned
group by City, Year(date)
order by year(date), rnk_cities;

# Compute moving average AQI per city

select City,year (Date),AQI,
    AVG(AQI) over (partition by City order by Date
        rows between 6 preceding and current row
    ) as moving_avg
from cleaned
order by city, year(Date);


-- Summary Table -- dql & Dml

Create table aqi_summary (
    City varchar(100),
    Year int,
    Avg_AQI Decimal(6,2),
    Max_AQI int,
    Min_AQI int,
    PRIMARY KEY (City, Year)
);

insert into aqi_summary (City, Year, Avg_AQI, Max_AQI, Min_AQI)
select
    City,
    YEAR(Date) AS Year,
    ROUND(AVG(AQI), 2) As Avg_AQI,
    MAX(AQI) As Max_AQI,
    MIN(AQI) As Min_AQI
from cleaned
group by City, Year(Date);
select * from aqi_summary;

-- wrost city per year
select City,Year,Avg_Aqi
from aqi_summary
order by Year, Avg_AQI DESC;

-- top 5 latest year pollutant cities
select City, Avg_AQI
from aqi_summary
 where Year = (select max(Year) from aqi_summary) 
#where year = 2020
order by Avg_AQI desc
limit 5;
