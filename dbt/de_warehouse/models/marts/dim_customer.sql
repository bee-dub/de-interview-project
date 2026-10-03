{{ config(materialized='table') }}

SELECT
	ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
	customer_id
FROM {{ ref('stg_customers') }}