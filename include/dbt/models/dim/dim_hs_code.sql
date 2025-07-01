WITH staging AS (
    SELECT * FROM {{ ref('stg_raw_table') }}
),

transform AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY hs_code
               ORDER BY 
                 CASE WHEN rubro IS NULL OR rubro = 'NT' THEN 1 ELSE 0 END,
                 CASE WHEN desc_capitulo IS NULL THEN 1 ELSE 0 END
           ) AS row_rank
    FROM staging
),

final AS(
    SELECT DISTINCT
        hs_code,
        rubro,
        desc_capitulo,
        desc_partida
    FROM transform
    WHERE row_rank = 1
)

SELECT * FROM final
