-- models/dim_repositorios.sql

{{ config(
    materialized='external',
    location='C:/010_/Trends-Watcher/output/gold/dim_repositorios.parquet'
) }}

SELECT 
    md5(repo_name) AS repo_id, -- Clave primaria que le digo que cree, baso en el nombre
    repo_name,
    total_stars,
    repo_language
FROM {{ ref('delta_silver') }} -- <--- dbt entiende que te refieres al sql silver que es el espejo del del bruto