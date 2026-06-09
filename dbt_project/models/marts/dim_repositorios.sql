-- models/dim_repositorios.sql

{{ config(materialized='incremental', unique_key='repo_name') }}

SELECT 
    md5(repo_name) AS id_repo, -- Clave primaria que le digo que cree, baso en el nombre
    repo_name,
FROM {{ ref('delta_silver') }} -- <--- dbt entiende que te refieres al sql silver que es el espejo del del bruto