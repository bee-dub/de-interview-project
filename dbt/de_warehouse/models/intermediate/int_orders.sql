{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT
    o.order_id,
    d.customer_key,
    o.customer_id,
    o.order_date,
    o.amount,
    o.updated_at

FROM {{ ref('stg_orders') }} o

LEFT JOIN {{ ref('dim_customer') }} d
    ON o.customer_id = d.customer_id

{% if is_incremental() %}

LEFT JOIN {{ this }} i
    ON o.order_id = i.order_id

WHERE i.order_id IS NULL 
    OR o.updated_at > i.updated_at

{% endif %}