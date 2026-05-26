{% macro confluent__get_table_columns_and_constraints() %}
  {#
    Override to fix dbt's Column.translate_type() mapping STRING → TEXT in the
    CREATE TABLE column list. We post-process the rendered constraints to restore
    STRING, since TEXT is not a valid Flink SQL type.
  #}
  {%- set raw_column_constraints = adapter.render_raw_columns_constraints(raw_columns=model['columns']) -%}
  {%- set raw_model_constraints = adapter.render_raw_model_constraints(raw_constraints=model['constraints']) -%}
  {%- set fixed_constraints = [] -%}
  {%- for c in raw_column_constraints -%}
    {%- set _ = fixed_constraints.append(c | replace(' TEXT', ' STRING')) -%}
  {%- endfor -%}
  (
  {% for c in fixed_constraints -%}
    {{ c }}{{ "," if not loop.last or raw_model_constraints }}
  {% endfor %}
  {% for c in raw_model_constraints -%}
      {{ c }}{{ "," if not loop.last }}
  {% endfor -%}
  )
{% endmacro %}


{% macro confluent__get_empty_schema_sql(columns) %}
  {#
    Override to fix dbt's Column.translate_type() mapping STRING → TEXT.
    TEXT is not a valid Flink SQL type; STRING is. We reverse the mapping here
    because dbt has already applied translate_type() before this macro runs.
  #}
  {{ validate_column_data_types(columns) }}
  {%- set flink_type_overrides = {'TEXT': 'STRING'} -%}
  {%- set col_exprs = [] -%}
  {%- for i in columns -%}
    {%- set col = columns[i] -%}
    {%- set col_name = col['name'] | replace('"', '') | replace("'", '') -%}
    {%- set raw_type = col.get('data_type', '') -%}
    {%- set data_type = flink_type_overrides.get(raw_type | upper, raw_type) -%}
    {%- set _ = col_exprs.append('cast(null as ' ~ data_type ~ ') as `' ~ col_name ~ '`') -%}
  {%- endfor -%}
  select {{ col_exprs | join(', ') }}
{% endmacro %}
