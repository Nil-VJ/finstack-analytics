{% macro pct_change(new_value, old_value, precision=2) %}
    case
        when {{ old_value }} is not null and {{ old_value }} != 0
        then round(({{ new_value }} - {{ old_value }}) / {{ old_value }} * 100, {{ precision }})
    end
{% endmacro %}
