WITH staging AS (
    SELECT * FROM {{ ref("stg_raw_table") }}
),

transform AS (
    SELECT
        year,
        month_name,
        CASE 
            WHEN mes = 'ENERO' THEN 1
            WHEN mes = 'FEBRERO' THEN 2
            WHEN mes = 'MARZO' THEN 3
            WHEN mes = 'ABRIL' THEN 4
            WHEN mes = 'MAYO' THEN 5
            WHEN mes = 'JUNIO' THEN 6
            WHEN mes = 'JULIO' THEN 7
            WHEN mes = 'AGOSTO' THEN 8
            WHEN mes = 'SEPTIEMBRE' THEN 9  
            WHEN mes = 'OCTUBRE' THEN 10
            WHEN mes = 'NOVIEMBRE' THEN 11
            WHEN mes = 'DICIEMBRE' THEN 12
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
