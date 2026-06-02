-- models/dim_time.sql

{{ config(
    materialized='external',
    location='C:/010_/Trends-Watcher/output/gold/dim_tiempo.parquet'
) }}

WITH date_series AS (
    SELECT DISTINCT
        
        CAST(extracted_at AS DATE) AS fecha_completa 
    FROM {{ ref('delta_silver') }}
)

SELECT
    -- Un PK numérico sencillo y estándar para fechas es el formato YYYYMMDD
    CAST(strftime('%Y%m%d', fecha_completa) AS INTEGER) AS id_tiempo,
    fecha_completa AS fecha,
    YEAR(fecha_completa) AS anio,
    MONTH(fecha_completa) AS mes,
    DAY(fecha_completa) AS dia,
    DAYOFWEEK(fecha_completa) AS dia_semana,
    STRFTIME('%B', fecha_completa) AS nombre_mes -- Ejemplo: 'January', 'February'
FROM date_series