with observed_dates as (
    select created_at::date as observed_date from {{ ref('int_pr_lifecycle') }}
    union all
    select resolved_at::date from {{ ref('int_pr_lifecycle') }} where resolved_at is not null
    union all
    select created_at::date from {{ ref('int_production_deployments') }}
    union all
    select successful_at::date
    from {{ ref('int_production_deployments') }}
    where successful_at is not null
),
bounds as (
    select min(observed_date) as start_date, max(observed_date) as end_date
    from observed_dates
),
date_spine as (
    select generate_series(start_date, end_date, interval '1 day')::date as date_day
    from bounds
    where start_date is not null and end_date is not null
)
select
    to_char(date_day, 'YYYYMMDD')::integer as date_key,
    date_day,
    extract(isoyear from date_day)::integer as iso_year,
    extract(quarter from date_day)::integer as calendar_quarter,
    extract(month from date_day)::integer as calendar_month,
    extract(week from date_day)::integer as iso_week,
    extract(isodow from date_day)::integer as iso_day_of_week,
    to_char(date_day, 'FMMonth') as month_name,
    to_char(date_day, 'FMDay') as day_name,
    extract(isodow from date_day) in (6, 7) as is_weekend
from date_spine
