-- dbt leerá esto y DuckDB escaneará la carpeta Delta directamente
SELECT
    id_venta AS venta_id,
    fecha_venta AS venta_fecha,
    id_cliente AS cliente_id,
    CAST(monto AS DOUBLE) AS monto_total
FROM delta_scan('../output/bronze/delta_bruto') 
-- Usamos 'delta_scan' porque DuckDB sabe leer formato Delta.