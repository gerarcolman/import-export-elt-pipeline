WITH staging AS (
    SELECT * FROM {{ ref('stg_raw_table') }}
),

transform AS (
    SELECT
        ROW_NUMBER() OVER(ORDER BY customs_name ASC) AS customs_id,
        customs_name
    FROM (
        SELECT DISTINCT customs_name
        FROM staging
    )
    ORDER BY customs_name
)

SELECT * FROM transform