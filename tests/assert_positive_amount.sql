-- Singular test: fail if any stg_orders row has a non-positive amount.
-- The WHERE filter in stg_orders should make this impossible,
-- but this test makes that contract explicit.
SELECT order_id
FROM {{ ref('stg_orders') }}
WHERE amount <= 0
