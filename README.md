# GitHub Trends Watcher

Proyecto para extraer, procesar y analizar tendencias de GitHub.
Este repositorio contiene un pipeline de procesamiento de datos desarrollado en **Scala** y **Apache Spark**. El objetivo del proyecto es limpiar datos crudos y almacenarlos de forma eficiente para su posterior modelado analítico.

## Estructura del Proyecto

- **data/** - Datos en formato CSV (ignorado en Git)
- **scripts_python/** - Fase 1: Extracción de datos
- **spark_scala/** - Fase 2: Procesamiento con Apache Spark
- **notebooks/** - Notebooks para pruebas rápidas

## Fases

1. **Extracción** - Obtener datos desde la API de GitHub
2. **Procesamiento** - Procesar y transformar datos con Spark
3. **Análisis** - Generar insights desde los datos procesados



# ///// AVANCE 2 ////
# 🚀  Conseguir que Spark, dentro del contenedor de Docker, genera Delta
---

## 🧠 Mapa de Batalla y Retos Técnicos

### 1. El Porqué del Proyecto (De Parquet a Delta Lake)
Inicialmente, el pipeline exportaba los datos limpios en formato Parquet. 
Decidí migrar a **Delta Lake** para aprovechar sus ventajas de rendimiento, transacciones ACID y el manejo de metadatos, preparando el terreno para el modelado de datos con **dbt**.
Un ejemplo específico es evitar duplicaciones (por errores de ejecución) usando **MERGE**, pero para poder usar MERGE se debe almacenar en formato Delta

### 2. Infraestructura con Docker (Evitando problemas de Hadoop en Windows)
Ejecutar Spark de forma nativa en Windows suele generar incompatibilidades críticas con los binarios de Hadoop (`winutils.exe`). Para solucionar esto, encapsulé todo el entorno de Spark y Scala dentro de un **contenedor de Docker**. El contenedor actúa como un entorno aislado que escribe directamente los resultados en el sistema de archivos local sin fricciones de compatibilidad.

Nota:
El .jar (tu código Scala) sabe que tiene que usar Delta porque acabo de agregar las configuraciones en el SparkSession, pero el entorno de Spark dentro de Docker no tiene el archivo físico de Delta Lake instalado para poder ejecutarlo, el contenedor es un Spark "limpio", no sabe qué es Delta, por eso hay que añadir en el Dockerfile en la última línea (CMD) el parámetro --packages para que spark-submit descargue la librería de Delta en tiempo de ejecución
```bash
CMD ["spark-submit", "--class", "ProcessTrends", "--packages", "io.delta:delta-spark_2.12:3.1.0", "/app/app.jar"]
```
(Hay que asegurarse mantener la versión 3.1.0 si usas Spark 3.5.0)

### 3. Gestión de Dependencias (SBT y Delta Lake)
Para habilitar el soporte de Delta en el pipeline, tuve que intervenir en varios niveles:
*   **`build.sbt`:** Añadí las dependencias de Delta Lake correspondientes a la versión de Spark utilizada.
*   **Código Scala (`.scala`):** Modifiqué la configuración del `SparkSession` para incluir las extensiones de Delta (`spark.sql.extensions`) y el catálogo de Delta. También actualicé la lógica de escritura sustituyendo `.format("parquet")` por `.format("delta")` y redefiniendo las rutas de salida.

### 4. Integración con el ecosistema de Datos (dbt y Databricks)
Para garantizar que el modelado posterior con **dbt** funcione sin conflictos de versiones, instalé mediante `pip` las librerías específicas compatibles con el entorno (asegurando el correcto acoplamiento entre dbt y los conectores de datos).

---

## ⚙️ Ciclo de Ejecución (Paso a Paso)

Dado que la lógica en Scala compila a un archivo `.jar` y este vive dentro de la imagen de Docker, no basta con modificar los archivos locales en la carpeta `target/`. El flujo estricto para aplicar cambios es:

1.  **Limpiar y Compilar el JAR:**
    ```bash
    sbt clean package
    *(según la configuración del plugin de empaquetado).*


2. **Reconstruir la Imagen de Docker:**
Forzar la reconstrucción para que el contenedor capture el nuevo .jar y las rutas actualizadas:
    ```bash
    docker build -t spark-docker-app:delta . ```
Consejo:
No toques el Dockerfile para nada, no muevas ni una coma, ni muevas espacios, ¡NADA!. 
De hecho no necesitas tocarlo para nada porque donde se hace la modificación o nombre de ruta de salida es en el archivo .scala donde se va a generar el Delta.
Docker sigue apuntando a la ruta donde está el .jar (no le importa que éste sea distinto) 
Pero si Docker detecta que ha cambiado algo en el Dockerfile no va a "aprovechar" la imagen anterior que ya tengas, va a descargar todo de nuevo y va a tardar como si fuera la primera vez.

3.  **Correr el Contenedor:**
    Ejecutar el proceso que disparará la limpieza en Spark y generará las tablas Delta:
    ```bash
    docker run --rm -v "${PWD}/../data:/app/data" -v "${PWD}/../output:/app/output" spark-docker-app:delta
    ```


# ///// AVANCE 3 ////

** Configurar DBT para que lea el Delta que generó Spark **
## 🛠️ Bitácora de Desafíos, Lecciones Aprendidas y Soluciones

### Guardar en Delta vs Consumo en Power BI
* **Problema:** Se requería formato **Delta Lake** en la ingesta para asegurar transacciones ACID y evitar duplicados mediante `MERGE`. Sin embargo, Power BI local no lee carpetas Delta del disco de forma nativa sin configurar conectores complejos o infraestructura en la nube.
* **Solución:** Se introdujo **DuckDB** como motor intermedio y **dbt** como orquestador de transformaciones. DuckDB actúa como un puente de alta velocidad: es capaz de leer las carpetas Delta de Spark y escribir el resultado procesado dentro de un archivo de base de datos relacional moderno (`.duckdb`), el cual sí es 100% compatible y ultra-rápido de leer desde Power BI.


### 3. Error de Compilación en dbt: `unexpected '.' line 9`
* **Problema:** Al ejecutar `dbt run`, el compilador de Jinja fallaba con un error sintáctico confuso apuntando a comentarios o puntos en el archivo `.sql` de staging.
* **Solución:** Se identificó que Jinja es sumamente estricto con los caracteres invisibles de codificación (originados al mover texto entre Docker/Windows) y con puntos específicos en los comentarios de cabecera. Se limpió el archivo a código SQL minimalista eliminando metadatos corruptos de texto.

### 4. Error de Extensión en dbt-duckdb: `Plugin delta not found` y `Table Function read_delta does not exist`
* **Problema:** Al intentar leer los datos de Spark, dbt fallaba debido a que DuckDB no reconocía las funciones nativas para leer Delta Lake (`delta_scan` o `read_delta`), indicando que el plugin no estaba cargado en la sesión.
* **Solución:** Intentar inyectar sentencias `INSTALL delta;` dentro del archivo SQL del modelo rompe el validador de dbt (que solo acepta sentencias `SELECT`). La solución robusta fue utilizar los ganchos de inicio globales (**`on-run-start`**) en el archivo `dbt_project.yml`:
  
```yaml
  on-run-start:
    - "INSTALL delta;"
    - "LOAD delta;"
```

Esto garantiza que DuckDB inicialice el soporte para Delta Lake en su memoria antes de evaluar cualquier script SQL.

### 5. Choque de Catálogo con sources.yml en Windows: Schema does not exist
* **Problema:**  Al utilizar la sintaxis estándar profesional de dbt {{ source('delta_spark', 'trends') }}, dbt-duckdb intentaba buscar un esquema lógico interno en la base de datos en lugar de mapear la ruta física del disco en Windows, provocando fallos de catálogo.

* **Solución:** Se migró la parametrización de rutas hacia el sistema de variables globales de dbt (vars). Al declarar la ruta en el dbt_project.yml, el código de staging quedó dinámico, limpio y totalmente funcional en Windows utilizando la función nativa de lectura:
```SQL
  SELECT * FROM delta_scan('{{ var("ruta_delta_bruto") }}')
```

### 6. Error de Columna Inexistente: Referenced column "updated_at" not found
* **Problema:**: El modelo de staging fallaba porque buscaba la columna updated_at directamente en el origen de datos (Delta bruto) para usarla como metadato de fecha de extracción, pero dicha columna no existía en el CSV original.

* **Solución:**  Se modificó la consulta para generar el metatato de auditoría en tiempo real directamente desde el motor de DuckDB utilizando la función de estampa temporal: now() AS extracted_at.

Estado Actual del Proyecto
Infraconstructura: Conectada y estable. dbt run ejecuta en verde (PASS) de manera consistente.

