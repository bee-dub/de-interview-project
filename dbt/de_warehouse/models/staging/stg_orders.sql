SELECT
    order_id,
    customer_id,
    order_date,
    amount, 
    updated_at
FROM {{ source('raw', 'orders') }}