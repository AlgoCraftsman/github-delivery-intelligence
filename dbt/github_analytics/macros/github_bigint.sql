{% macro github_bigint(value) -%}
    case
        when ({{ value }}) ~ '^[0-9]+$'
            then ({{ value }})::bigint
        else null
    end
{%- endmacro %}
