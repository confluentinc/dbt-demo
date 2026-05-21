{{
  config(
    materialized        = 'materialized_table',
    distributed_by      = 'order_id',
    start_mode          = 'RESUME_OR_FROM_BEGINNING',
    with                = {
      'key.format':   'avro-registry',
      'value.format': 'avro-registry'
    }
  )
}}

-- Continuously refreshed stream of high-value orders. Change the threshold
-- in the WHERE clause below and run:
--   dbt run --full-refresh -s high_value_orders
-- to update the filter logic. Flink owns the refresh pipeline; dbt manages
-- the lifecycle.
SELECT
    order_id,
    customer_id,
    product_id,
    product_name,
    CAST(amount AS DOUBLE) AS amount,
    status,
    event_time
FROM {{ ref('stg_orders') }}
WHERE amount >= 200
