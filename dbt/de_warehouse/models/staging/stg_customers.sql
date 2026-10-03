SELECT DISTINCT
    customer_id
FROM {{ source('raw', 'orders') }}