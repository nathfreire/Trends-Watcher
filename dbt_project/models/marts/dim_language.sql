-- models/dim_language.sql

{{ config(
    materialized='external',
    location='C:/010_/Trends-Watcher/output/gold/dim_language.parquet'
) }}

WITH unique_languages AS (
    SELECT DISTINCT
        repo_language
    FROM {{ ref('delta_silver') }}
)

SELECT 
    ROW_NUMBER() OVER () AS id, -- Genera un PK secuencial sencillo (1, 2, 3...)
    repo_language
FROM unique_languages