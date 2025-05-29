{% macro limit_data_in_dev(column_name, dev_in_days_of_data) %}

{% if target.name == 'dev' %}
where {{ column_name }} >= dateadd('day', -{{ dev_days_of_data }}, current_timestamp)
{% endif %}
{% endmacro %}