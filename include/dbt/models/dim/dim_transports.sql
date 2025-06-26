WITH staging AS (
    SELECT * FROM {{ ref('stg_raw_table') }}
),

transform AS (
    SELECT
        ROW_NUMBER() OVER(ORDER BY transport_type ASC) AS transport_id,
        transport_type
    FROM
        (SELECT DISTINCT transport_type
         FROM staging)
    ORDER BY transport_type ASC
)

SELECT * FROM transform