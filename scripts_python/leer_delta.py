import duckdb

# Conectamos DuckDB (creará una base de datos temporal en memoria)
with duckdb.connect() as con:
    # Instalamos y cargamos la extensión necesaria para leer formato Delta Lake
    con.execute("INSTALL delta;")
    con.execute("LOAD delta;")

    path_al_delta = "C:/010_/Trends-Watcher/output/bronze/delta_bruto"

    print("--- ESTRUCTURA Y COLUMNAS ---")
    con.sql(f"DESCRIBE SELECT * FROM delta_scan('{path_al_delta}');").show()

    print("\n--- PRIMEROS 5 REGISTROS ---")
    # Cambiamos 'stars' por 'stargazers_count' (y le ponemos un alias opcional si quieres ver "stars" en la tabla)
    con.sql(f"SELECT id, name, stargazers_count AS stars, owner_login FROM delta_scan('{path_al_delta}') LIMIT 5;").show()
    