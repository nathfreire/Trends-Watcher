-- models/dim_repositorios.sql

{{ config(materialized='external', location='C:/010_/Trends-Watcher/output/gold/dim_repositorios.parquet', unique_key='repo_name', incremental_strategy='delete+insert') }}

SELECT 
    md5(repo_name) AS id_repo, -- Clave primaria que le digo que cree, baso en el nombre
    repo_name,
FROM {{ ref('delta_silver') }} -- <--- dbt entiende que te refieres al sql silver que es el espejo del del bruto