with ytd_calendar as (
  -- Because the tune_looker table doesn't contain data before March 2022, I am missing dates when adding a year.
  -- Creating the calendar in stages solves that problem
  SELECT DISTINCT
    date as future_date,
    EXTRACT(DAYOFWEEK FROM date) as dayofweek, -- extract the day of week
    EXTRACT(MONTH FROM date) as month -- extract the month
  FROM `prod_tune.tune_looker`
  WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM current_date)
),
future_calendar as (
      -- Because the future dates are not in the table, have to use the dateadd function
    SELECT DISTINCT
        DATE_ADD(date, INTERVAL 1 year) as future_date, -- Create a list of dates for the current month
        EXTRACT(DAYOFWEEK FROM DATE_ADD(date, INTERVAL 1 year)) as dayofweek, -- extract the day of week
        EXTRACT(MONTH FROM DATE_ADD(date, INTERVAL 1 year)) as month -- extract the month
    FROM `prod_tune.tune_looker`
    WHERE DATE_ADD(date, INTERVAL 1 YEAR) BETWEEN DATE_TRUNC(current_date,YEAR) -- Starting at the beginning of the year
                            AND LAST_DAY(current_date,YEAR) -- Until the end of the year
    ORDER BY 1 DESC
),
month_calendar as (
  -- Union data to get the full year calendar 
  SELECT DISTINCT * FROM
  (
    SELECT * FROM ytd_calendar
    UNION ALL
    SELECT * FROM future_calendar
  )
),
days_in_current_month as (
    -- GROUP BY the dayofweek to get the number of each day in the current month
    SELECT month, dayofweek,
    count(*) as days_in_month
    FROM month_calendar
    GROUP BY 1,2
),
monthly_budget as (
  -- Get the budget from the correct table
    SELECT
    year,
    period as month,
    brand_domain, budget, current_reforecast
    FROM [HIDDEN_TABLE_NAME]
    -- LEFT JOIN brand_by_dayofweek on brand_domain = Brand
    WHERE YEAR = EXTRACT(year FROM current_date)
    -- AND period = EXTRACT(month FROM current_date)
    ORDER BY 1
),
actual_revenue as (
  -- To enable forecasting, get the current revenue numbers for the year
  SELECT
  date as event_date,
  EXTRACT(MONTH FROM date) as month,
  Brand,
  sum(revenue) as revenue,
  sum(CASE WHEN date = current_date THEN NULL WHEN revenue_type != "Addition" THEN revenue ELSE 0 END) as revenue_to_pace
  FROM [HIDDEN_TABLE_NAME]
  WHERE date BETWEEN DATE_TRUNC(current_date,YEAR) -- Starting at the beginning of the year
                            AND current_date -- Until today
  GROUP BY 1,2,3

  UNION ALL
  -- Add in the future dates to make sure the left table contains all needed dates for the year
  SELECT future_date as event_date,
  month,
  b.brand_domain as Brand,
  CAST(NULL as NUMERIC) as revenue,
  CAST(NULL as NUMERIC) as revenue_to_pace
  FROM month_calendar a
  CROSS JOIN (SELECT DISTINCT brand_domain FROM monthly_budget WHERE brand_domain IS NOT NULL) b
  WHERE future_date BETWEEN DATE_ADD(current_date, INTERVAL 1 DAY) -- Tomorrow
                    AND LAST_DAY(current_date,YEAR) -- Until the end of the year
),
actual_daily_average_by_month as (
  -- Group by the month and brand
  SELECT month,
  Brand,
  AVG(revenue_to_pace) as monthly_average_revenue
  FROM actual_revenue
  group by 1,2
),
actual_revenue_with_forecast as (
    -- Combine the actual revenue with the monthly average and then create a forecast column
    -- that contains both the actual and the forecasted revenue
    SELECT
        a.event_date,
        a.month,
        a.Brand,
        a.revenue as actual_revenue,
        b.monthly_average_revenue,
        CASE WHEN event_date < current_date THEN revenue
        WHEN EXTRACT(MONTH FROM event_date) = EXTRACT(MONTH FROM current_date) THEN monthly_average_revenue
        ELSE revenue END as actual_plus_forecast_revenue
    FROM actual_revenue a
    LEFT JOIN actual_daily_average_by_month b on a.month = b.month and a.Brand = b.Brand
    ORDER BY 1
),
raw_clicks as (
  -- Get the daily seasonality of the clicks
  -- Use window functions to create totals and subtotals within queries
  SELECT DISTINCT
  Brand,
  EXTRACT(DAYOFWEEK FROM date) as dayofweek, -- extract the day of week
  (1.0 * -- CAST as FLOAT
   count(*) OVER (PARTITION BY Brand,EXTRACT(DAYOFWEEK FROM date)) / -- dow_total_clicks
   count(*) OVER (PARTITION BY Brand)) as weekly_rate   -- total_clicks
  FROM [HIDDEN_TABLE_NAME]
  WHERE date between DATE_ADD(date, INTERVAL -3 MONTH) AND current_date
),
daily_weights as (
  -- Get the proportion of revenue-driving events for a given day
    SELECT
    a.*, -- Get everything from the raw_clicks table
    b.month,
    b.days_in_month, -- Join in the days of the month
    (1.0 *
    ((a.weekly_rate * b.days_in_month) / SUM((a.weekly_rate * b.days_in_month)) OVER (PARTITION BY Brand, b.month)) /
     b.days_in_month) as daily_weighted_percent, -- The total percent over days in month gives daily weight
    -- (SELECT budget FROM monthly_budget) as budget, -- Pull in budget from Sox Hyperion
    -- (daily_weighted_percent * budget) as daily_target, -- Daily Weight * Budget for each day
    -- (daily_target * days_in_month) as monthly_target -- The total contribution of a day for the month
FROM raw_clicks a
JOIN days_in_current_month b on a.dayofweek = b.dayofweek
ORDER BY 1
),
dayofweek_budget as (
  -- Join in the budget and apply the proportions
  SELECT
  a.*,
  b.budget,
  b.current_reforecast,
  (daily_weighted_percent * budget) as budget_daily_target, -- Daily Weight * Budget for each day
  (daily_weighted_percent * budget * days_in_month) as budget_monthly_target, -- The total contribution of a day for the month
  (daily_weighted_percent * current_reforecast) as reforecast_daily_target,
  FROM daily_weights a
  LEFT JOIN monthly_budget b on a.Brand = b.brand_domain and a.month = b.month
  ORDER BY 1,2
)
-- Join everything together for a final governed data source
SELECT
    b.Brand,
    -- a.month as event_month,
    a.future_date as event_date,
    a.dayofweek as dayofweek,
    b.budget_daily_target,
    c.budget as total_budget,
    b.reforecast_daily_target,
    c.current_reforecast as total_reforecast,
    d.actual_revenue,
    d.actual_plus_forecast_revenue
FROM month_calendar a
LEFT JOIN dayofweek_budget b on a.dayofweek = b.dayofweek and a.month = b.month
LEFT JOIN monthly_budget c on b.Brand = c.brand_domain and a.month = c.month
LEFT JOIN actual_revenue_with_forecast d on d.event_date = a.future_date and b.Brand = d.Brand
ORDER BY 1,2
