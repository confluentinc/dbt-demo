{% materialization streaming_source, adapter='confluent' %}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type=this.Table) %}

  {%- set connector = config.get('connector') -%}
  {% if not connector %}
    {% set msg="'connector' must be specified in 'streaming_source' materialization" %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}
  {%- set with_options = config.get('with', {}) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% if skip_or_drop_existing(existing_relation, target_relation, has_select_query=false) %}
    {% call noop_statement('main', 'SKIP') %}{% endcall %}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {{ return({'relations': [target_relation]}) }}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main', execution_mode="streaming_ddl",
                    statement_name=get_statement_name()) -%}
    CREATE TABLE {{ target_relation }}
    ( {{ sql }})
    WITH (
      'connector' = '{{ connector }}'
      {%- for key, value in with_options.items() -%}
      , '{{ key }}' = '{{ value | replace("'", "''") }}'
      {%- endfor -%}
    )
  {%- endcall %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
