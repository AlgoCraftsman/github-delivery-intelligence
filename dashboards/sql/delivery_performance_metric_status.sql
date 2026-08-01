-- Grain: one repository and metric on the latest fixture reporting date.
-- Visualization: table placed beside delivery-performance trend cards.
-- Filters: repository_full_name and metric_name.
-- Assumption: unavailable values remain rows and are never coerced to zero.
with latest_reporting_date as (
    select max(date_day) as date_day
    from analytics_marts.fct_delivery_performance_daily
)
select
    metrics.repository_full_name,
    metrics.date_day,
    metrics.metric_name,
    metrics.metric_value,
    metrics.measurement_status,
    metrics.coverage_numerator,
    metrics.coverage_denominator,
    metrics.coverage_ratio,
    metrics.definition_version,
    metrics.exclusion_reason,
    case
        when metrics.measurement_status = 'unavailable' then 'Unavailable'
        when metrics.measurement_status = 'configured_proxy' then 'Configured proxy'
        else 'Measured'
    end as status_label
from analytics_marts.fct_delivery_performance_daily as metrics
inner join latest_reporting_date using (date_day)
order by metrics.repository_full_name, metrics.metric_name;
