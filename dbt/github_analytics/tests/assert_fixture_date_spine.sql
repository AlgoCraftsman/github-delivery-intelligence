{% if var('fixture_validation', false) %}

select
    min(date_day) as actual_start_date,
    max(date_day) as actual_end_date,
    count(*) as actual_date_count
from {{ ref('dim_date') }}
having min(date_day) is distinct from '2026-01-10'::date
    or max(date_day) is distinct from '2026-01-13'::date
    or count(*) is distinct from 4::bigint

{% else %}

select 1 as fixture_validation_disabled
where false

{% endif %}

