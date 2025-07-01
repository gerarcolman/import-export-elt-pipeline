WITH staging AS (
    SELECT * FROM {{ ref("stg_raw_table") }}
),

transform AS (
    SELECT DISTINCT
        year,
        month_name,
        CASE 
            WHEN month_name = 'ENERO' THEN 1
            WHEN month_name = 'FEBRERO' THEN 2
            WHEN month_name = 'MARZO' THEN 3
            WHEN month_name = 'ABRIL' THEN 4
            WHEN month_name = 'MAYO' THEN 5
            WHEN month_name = 'JUNIO' THEN 6
            WHEN month_name = 'JULIO' THEN 7
            WHEN month_name = 'AGOSTO' THEN 8
            WHEN month_name = 'SEPTIEMBRE' THEN 9  
            WHEN month_name = 'OCTUBRE' THEN 10
            WHEN month_name = 'NOVIEMBRE' THEN 11
            WHEN month_name = 'DICIEMBRE' THEN 12
        END AS month_number

    FROM staging

),

final AS (

    SELECT
        DATE(year, month_number, 1) AS date_id,
        year,
        month_number,
        month_name
    FROM transform

)

SELECT * FROM final
