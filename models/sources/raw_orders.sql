{{
  config(
    materialized = 'streaming_source',
    connector    = 'faker',
    with = {
      'rows-per-second':                      '5',
      'fields.order_id.expression':           "#{number.numberBetween ''1'',''1000000''}",
      'fields.customer_id.expression':        "#{number.numberBetween ''1'',''500''}",
      'fields.product_id.expression':         "#{number.numberBetween ''1'',''50''}",
      'fields.product_name.expression':       "#{Commerce.productName}",
      'fields.amount.expression':             "#{number.randomDouble ''2'',''5'',''500''}",
      'fields.status.expression':             "#{options.option ''placed'',''processing'',''shipped'',''delivered''}",
      'fields.event_time.expression':         "#{date.past ''30'',''SECONDS''}"
    }
  )
}}

order_id      BIGINT NOT NULL,
customer_id   BIGINT NOT NULL,
product_id    BIGINT NOT NULL,
product_name  STRING NOT NULL,
amount        DECIMAL(10, 2) NOT NULL,
status        STRING NOT NULL,
event_time    TIMESTAMP(3) NOT NULL
