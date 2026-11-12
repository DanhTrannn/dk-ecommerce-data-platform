SELECT
  CAST(b."date_key" AS integer) AS "date_key",
  CAST(b."full_date" AS date) AS "full_date",
  CAST(b."day_of_week" AS integer) AS "day_of_week",
  CAST(b."day_name" AS varchar) AS "day_name",
  CAST(b."day_of_month" AS integer) AS "day_of_month",
  CAST(b."day_of_year" AS integer) AS "day_of_year",
  CAST(b."week_of_year" AS integer) AS "week_of_year",
  CAST(b."month" AS integer) AS "month",
  CAST(b."month_name" AS varchar) AS "month_name",
  CAST(b."quarter" AS integer) AS "quarter",
  CAST(b."year" AS integer) AS "year",
  CAST(b."is_weekend" AS boolean) AS "is_weekend",
  CAST(b."is_holiday" AS boolean) AS "is_holiday",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at"
FROM (
SELECT day full_date,CAST(date_format(CAST(day AS timestamp),'%Y%m%d') AS integer) date_key,
(day_of_week(day)%7)+1 day_of_week,date_format(CAST(day AS timestamp),'%W') day_name,
day_of_month(day) day_of_month,day_of_year(day) day_of_year,week(day) week_of_year,
month(day) month,date_format(CAST(day AS timestamp),'%M') month_name,quarter(day) quarter,year(day) year,
day_of_week(day) IN (6,7) is_weekend,false is_holiday
FROM UNNEST(sequence(DATE '2025-01-01',DATE '2027-12-31',INTERVAL '1' DAY)) AS dates(day)
) b
