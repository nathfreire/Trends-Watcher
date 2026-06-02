-- models/facts.sql

{{ config(
    materialized='external',
    location='C:/010_/Trends-Watcher/output/gold/facts.parquet'
) }}

SELECT
    -- 1. Dimensión Repositorio: Usamos MD5 (Pasa directo de silver)
    md5(silver.repo_name) AS id_repositorio, 
    
    -- 2. Dimensión Tiempo: Formato YYYYMMDD (Se calcula directo de la fecha)
    CAST(strftime('%Y%m%d', CAST(silver.extracted_at AS DATE)) AS INTEGER) AS id_tiempo,
    
    -- 3. Dimensión Lenguaje: Traemos el ID secuencial desde su dimensión
    dim_lang.id_lang AS id_lang, 
    
    -- Métricas (Hechos)
    silver.total_stars AS cantidad_estrellas

FROM {{ ref('delta_silver') }} AS silver

-- Hacemos el JOIN con la dimensión para heredar el ID numérico correcto (1, 2, 3...)
LEFT JOIN {{ ref('dim_language') }} AS dim_lang
    ON silver.repo_language = dim_lang.repo_language