WITH staging AS (
    SELECT * FROM {{ ref('stg_raw_table') }}
),

transform AS (
    SELECT DISTINCT
        hs_code,
        rubro,
        desc_capitulo,
        desc_partida
        
    FROM staging
    ORDER BY hs_code ASC
)

SELECT * FROM transform