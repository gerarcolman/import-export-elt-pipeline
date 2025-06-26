WITH staging AS (
    SELECT * FROM {{ ref('stg_raw_table') }}
),

transform AS (
    SELECT 
        ROW_NUMBER() OVER(ORDER BY measurement_name) AS measurement_id,
        measurement_name 
    FROM (SELECT DISTINCT measurement_name
          FROM staging)

    ORDER BY measurement_name ASC
)

SELECT * FROM transform