WITH source AS (
    SELECT * FROM {{ source('raw_import_export', 'datos_abiertos_raw') }}
),

transform AS (
    SELECT
        despacho_cifrado AS clearance_id, 
        operacion AS operation,	
        destinacion AS operation_id,	
        regimen AS regime,
        `año` AS year,
        mes AS month_name,
        aduana AS customs_name,		
        CASE 
            WHEN medio_transporte IS NULL AND aduana IN ('AEROP. PETTIROSSI', 'AEROPUERTO GUARANI') THEN 'AVION'
            WHEN medio_transporte IS NULL AND aduana = 'CIUDAD DEL ESTE' AND (pais_origen = 'BRASIL' OR pais_procedenciadestino = 'BRASIL') THEN 'CAMION'
            WHEN medio_transporte IS NULL AND aduana IN ('CAMPESTRE S.A.', 'INFANTE RIVAROLA', 'JOSE FALCON', 'MARISCAL ESTIGARRIBIA', 'PEDRO JUAN CABALLERO', 'PUERTO SECO BOREAL') THEN 'CAMION'
            ELSE medio_transporte
        END AS transport_type,
        canal AS channel,	
        TRIM(
            CASE
                WHEN REGEXP_CONTAINS(pais_origen, r'^[A-Z]{2} - ') THEN SUBSTR(pais_origen, 6) 
                ELSE pais_origen 
            END
        ) AS country_origin,
        TRIM(
            CASE
                WHEN REGEXP_CONTAINS(pais_procedenciadestino, r'^[A-Z]{2} - ') THEN SUBSTR(pais_procedenciadestino, 6) 
                ELSE pais_procedenciadestino 
            END
        ) AS country_origin_destination,	
        uso AS usage,	
        CASE
            WHEN unidad_medida_estadistica = 'KG.BRUTO' THEN 'KILOGRAMO'
            ELSE unidad_medida_estadistica
        END AS measurement_name,	
        cantidad_estadistica AS quantity,	
        CASE
            WHEN cantidad_subitem <> 0 AND precion_unitario_subitem <> 0 THEN (cantidad_subitem * precion_unitario_subitem)
            ELSE fob_dolar
        END AS usd_fob,			
        posicion AS hs_code,
        rubro,
        desc_capitulo,
        desc_partida,	
        mercaderia AS item,
        desc_subitem AS subitem,	
        TRIM(UPPER(COALESCE(marca_item, marca_subitem, 'SIN MARCA'))) AS brand,	
        acuerdo
    FROM source
)

SELECT * FROM transform