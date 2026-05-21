-- models/staging/delta_silver.sql

WITH raw_data AS (
    -- dbt traducirá esto automáticamente usando la configuración de tu sources.yml
    SELECT * FROM delta_scan('{{ var("ruta_delta_bruto") }}')
)

SELECT
    TRIM(name) AS repo_name,
    CAST(stars AS INT) AS total_stars,
    LOWER(language) AS repo_language,
    now() AS extracted_at
FROM raw_data