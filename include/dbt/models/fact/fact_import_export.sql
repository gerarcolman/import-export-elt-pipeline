WITH stg_raw_table AS (
    SELECT * FROM {{ ref('stg_raw_table') }}
),

countries AS (
    SELECT * FROM {{ ref('countries') }}
),

customs_regimes AS (
    SELECT * FROM {{ ref('customs_regimes') }}
),

customs AS (
    SELECT * FROM {{ ref('dim_customs') }}
),

transports AS (
    SELECT * FROM {{ ref('dim_transports') }}
),

hs_code AS (
    SELECT * FROM {{ ref('dim_hs_code') }}
),

measurements AS (
    SELECT * FROM {{ ref('dim_measurements') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
),

final_fct AS (
    SELECT
        stg.operation_id,
        dates.date_id,
        customs.customs_id, 
        transports.transport_id,
        stg.channel,
        c1.country_id AS country_origin_id,
        c2.country_id AS country_origin_destination_id,
        stg.usage,
        measurements.measurement_id,
        stg.quantity,
        stg.usd_fob,
        stg.usd_freight,
        stg.usd_insurance,
        stg.usd_totals,
        hs_code.hs_code,
        stg.merchandise,
        stg.brand,
        stg.acuerdo

    FROM stg_raw_table AS stg
    LEFT JOIN customs_regimes 
    ON stg.operation_id=customs_regimes.operation_id
    LEFT JOIN customs
    ON stg.customs_name=customs.customs_name
    LEFT JOIN transports
    ON stg.transport_type=transports.transport_type
    LEFT JOIN countries AS c1
    ON stg.country_origin=c1.country_name
    LEFT JOIN countries AS c2
    ON stg.country_origin_destination=c2.country_name
    LEFT JOIN measurements
    ON stg.measurement_name=measurements.measurement_name
    LEFT JOIN hs_code
    ON stg.hs_code=hs_code.hs_code
    LEFT JOIN dates
    ON stg.year=dates.year
    AND stg.month_name=dates.month_name

)

SELECT * FROM final_fct