SELECT
    s.customer_id
FROM {{ ref('stg_customers') }} s
LEFT JOIN {{ ref('dim_customer') }} d
    ON s.customer_id = d.customer_id
WHERE d.customer_id IS NULL


