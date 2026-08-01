-- Grain: one repository and repository-local calendar date.
-- Visualization: line chart; split series by repository_full_name.
-- Filters: repository_full_name and date_day.
-- Assumption: configured empty dates are measured zeroes from the contracted mart.
select
    repository_full_name,
    date_day,
    metric_value as deployments,
    measurement_status,
    coverage_numerator,
    coverage_denominator,
    coverage_ratio,
    definition_version,
    exclusion_reason
from analytics_marts.fct_delivery_performance_daily
where metric_name = 'deployment_frequency'
order by repository_full_name, date_day;
