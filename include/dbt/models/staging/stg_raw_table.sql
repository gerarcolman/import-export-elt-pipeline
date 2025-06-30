WITH source AS (
    SELECT * FROM {{ source('raw_import_export_2025', 'datos_abiertos_raw') }}
),

transform AS (
    SELECT 
        operacion AS operation,	
        destinacion AS operation_id,	
        regimen AS regime,
        `año` AS year,
        mes AS month_name,
        aduana AS customs_name,	
        cotizacion,	
        CASE 
            WHEN medio_transporte IS NULL AND aduana IN ('AEROP. PETTIROSSI', 'AEROPUERTO GUARANI') THEN 'AVION'
            WHEN medio_transporte IS NULL AND aduana = 'CIUDAD DEL ESTE' AND (pais_origen = 'BRASIL' OR pais_procedenciadestino = 'BRASIL') THEN 'CAMION'
            WHEN medio_transporte IS NULL AND aduana IN ('CAMPESTRE S.A.', 'INFANTE RIVAROLA', 'JOSE FALCON', 'MARISCAL ESTIGARRIBIA', 'PEDRO JUAN CABALLERO', 'PUERTO SECO BOREAL') THEN 'CAMION'
            ELSE medio_transporte
        END AS transport_type,
        canal AS channel,
        item,	
        pais_origen AS country_origin,	
        pais_procedenciadestino AS country_origin_destination,	
        uso AS usage,	
        CASE
            WHEN unidad_medida_estadistica = 'KG.BRUTO' THEN 'KILOGRAMO'
            ELSE unidad_medida_estadistica
        END AS measurement_name,	
        cantidad_estadistica AS quantity,	
        fob_dolar AS usd_fob,	
        flete_dolar AS usd_freight,	
        seguro_dolar AS usd_insurance,	
        imponible_dolar AS usd_totals,	
        imponible_gs AS gs_totals,
        posicion AS hs_code,
        rubro,
        desc_capitulo,
        desc_partida,	
        mercaderia AS merchandise,	
        marca_item AS brand,	
        acuerdo
    FROM source
)

SELECT * FROM transform