-- models/dim_language.sql

{{ config(
    materialized='incremental',
    unique_key='repo_language'
) }}

WITH unique_languages AS (
    SELECT DISTINCT
        repo_language
    FROM {{ ref('delta_silver') }}
)

SELECT 
    ROW_NUMBER() OVER () AS id_lang, -- Genera un PK secuencial sencillo (1, 2, 3...)
    repo_language
FROM unique_languages