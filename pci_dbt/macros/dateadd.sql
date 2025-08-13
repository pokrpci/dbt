{% macro dateadd(datepart, number, date) %}
  select dateadd({{ datepart }}, {{ number }}, {{ date }})
{% endmacro %}
