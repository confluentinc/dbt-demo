{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    product_id,
    product_name,
    LOWER(TRIM(status)) AS status,
    amount,
    event_time
FROM {{ ref('raw_orders') }}
WHERE amount > 0
