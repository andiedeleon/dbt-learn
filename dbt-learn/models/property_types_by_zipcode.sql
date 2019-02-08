{% set property_types = dbt_learn.get_column_values_alphabetically(
  table="source_data.listings",
  column='property_type',
  max_records=10)
%}

{{ log(property_types, info=True) }}

select
  zipcode,
  {% for property_type in property_types %}
    {% set property_type_clean = property_type | lower |
      replace(" ", "_") | replace("/", "_") %}

    sum(case when property_type = '{{property_type}}' then 1 else 0 end)
      as {{property_type_clean}}_count

    {{-"," if not loop.last-}}

  {%- endfor %}

from source_data.listings

group by 1

limit 100
