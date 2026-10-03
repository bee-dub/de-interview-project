{{ config(materialized='table') }}

SELECT DISTINCT 
    TO_CHAR(order_date, 'YYYYMMDD'):: INTEGER AS date_key,
    order_date AS date,
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month,
    EXTRACT(QUARTER FROM order_date) AS quarter
FROM {{ ref('stg_orders') }}