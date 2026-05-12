@echo off
:: La consola abre en System32, Para que no ejecute desde System32 si no
:: desde a la carpeta del script
cd /d "%~dp0"
echo [1/2] Extrayendo datos de la API de GitHub...
python scripts_python\extract_api.py

echo [2/2] Procesando datos con Spark en Docker...
:: Usamos %~dp0 también aquí para asegurar las rutas de los volúmenes
:: Añadimos "2> error_log.txt" para capturar errores si los hay

docker run -v "%~dp0data:/app/data" -v "%~dp0output:/app/output" spark-docker-app > pipeline_log.txt 2>&1
echo ¡Pipeline completado! Revisa la carpeta Output.
