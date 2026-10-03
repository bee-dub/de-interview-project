{{ config(materialized='table') }}

SELECT 
    order_id,
    customer_key,
    d.date_key,
    amount
FROM {{ ref('int_orders') }} o
LEFT JOIN {{ ref('dim_date') }} d
    ON o.order_date = d.date