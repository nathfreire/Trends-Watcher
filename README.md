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


---

# ///// AVANCE 4  ////
## 🛠️ El Dilema Arquitectónico: Almacenamiento y Conectividad

Actualmente, el proyecto se encuentra en una fase crucial de arquitectura: **definir la estrategia de almacenamiento local y la conectividad óptima con Power BI** para la capa de visualización.

Al trabajar en un entorno de desarrollo puramente local (en mi ordenador), se han identificado limitaciones con formatos de almacenamiento como **Delta Lake** (ya que DuckDB es excelente leyendo Delta, pero no está optimizado para escribir en este formato de forma nativa). 

Por lo tanto, el flujo de datos hacia Power BI se encuentra ante una bifurcación con **dos caminos posibles**:

### 🗺️ Opción 1: Consumo Directo vía Archivos Parquet (Ruta Absoluta)
Consiste en configurar el pipeline para que dbt exporte de manera externa las dimensiones y hechos en archivos planos con formato `.parquet` en un directorio local específico.
* **Flujo:** `dbt` ➡️ Archivos `.parquet` locales ➡️ `Power BI` (Lectura directa apuntando a la ruta absoluta).
* **Desventaja:** Implica una gestión externa de archivos y dependemos de rutas rígidas dentro del sistema operativo del ordenador.

### 🔌 Opción 2: Almacenamiento Nativo e Integración por Conector DuckDB
Consiste en mantener los datos procesados resguardados de forma centralizada dentro de la propia base de datos interna y binaria de DuckDB, instalando un conector específico para Power BI.
* **Flujo:** `dbt` ➡️ Base de datos interna (`.db` / `.duckdb`) ➡️ `Power BI` (A través del conector de DuckDB).
* **Configuración en dbt:** Requiere modificar el archivo de configuración `dbt_project.yml` (y las propiedades del modelo) para especificar que la materialización sea puramente interna (`materialized='table'`). De esta manera, el sistema no exporta hacia fuera (ni a Parquet ni a Delta).
* **Ventaja Clave:** Al delegar el almacenamiento a la base de datos interna, DuckDB utiliza sus propios estándares de guardado de forma nativa. Esto garantiza mecanismos automáticos de **consistencia de datos, integridad y rendimiento analítico**, abstrayendo por completo la gestión manual de archivos en disco.

---

## 📊 Matriz Comparativa de Alternativas

| Criterio | Opción 1: Archivos Parquet Externos | Opción 2: Base de Datos Interna DuckDB |
| :--- | :--- | :--- |
| **Ubicación de Datos** | Archivos independientes en el disco duro. | Dentro del archivo de base de datos de DuckDB. |
| **Configuración dbt** | Requiere configuraciones de exportación física. | Configuración limpia en `dbt_project.yml` (`table`). |
| **Consistencia** | Manual (Riesgo de desincronización de archivos). | **Nativa y Automática** (Gestionada por el motor). |
| **Dependencia Power BI** | Ninguna (Conector Parquet integrado de fábrica). | Requiere instalar el conector de DuckDB en la máquina. |

---

## 🚀 Próximos Pasos y Plan de Acción

1. **Testear el Conector:** Instalar el conector de DuckDB en Power BI local para evaluar su estabilidad y velocidad de respuesta.
2. **Definir el `dbt_project.yml`:** Una vez seleccionado el camino, estructurar el archivo YAML para estandarizar las materializaciones del proyecto de forma definitiva.
3. **Validación del Modelo:** Realizar cargas completas de los datos de los repositorios para certificar que las tablas de hechos (*facts*) y dimensiones se vinculen correctamente en el modelo estelar de Power BI.

# //// AVANCE 5 ////

### Choque de Catálogo con `sources.yml` en Entornos Sin Metastore (DuckDB)

* **Problema:** Al utilizar la sintaxis estándar y recomendada de dbt `{{ source('bronze', 'delta_bruto') }}`, la compilación fallaba en el modelo de staging (`delta_silver.sql`), es decir cuando este `.sql` aplica ese source(), llama a su fuente source.yml y salta con un error de plugin o de tabla inexistente. 
 :

    ```text
    11:23:05  Failure in model delta_silver (models\staging\delta_silver.sql)
    11:23:05    Compilation Error in model delta_silver (models\staging\delta_silver.sql)
      Plugin delta not found; known plugins are: 
    11:23:05  
    11:23:05  Done. PASS=2 WARN=0 ERROR=1 SKIP=4 NO-OP=0 TOTAL=7
    ```

* **Causa Raíz:** Este error representa una limitación conocida en el stack de ficheros locales sin un metastore centralizado (como Hive Metastore o Databricks Catalog). 
    
    Cuando usas el adaptador `dbt-duckdb`, el macro `{{ source('bronze', 'delta_bruto') }}` intenta traducir la consulta a un formato relacional estándar (ej. `SELECT * FROM main.delta_bruto`). DuckDB busca esa tabla dentro de su catálogo en memoria, pero **no existe**, ya que en realidad es un directorio de archivos Delta externos en el disco duro de Windows que requiere la función especializada `delta_scan()` de forma explícita.

* **Solución:** Para solucionarlo, tuve que forzar el uso de la función nativa de DuckDB, delta_scan() en vez de `{{ source('bronze', 'delta_bruto') }} :

    ```sql
    -- En models/staging/delta_silver.sql
    SELECT * FROM delta_scan('{{ var("ruta_delta_bruto") }}') -- pasándole la ruta absoluta mediante una variable global declarada en `dbt_project.yml`,`
    ```


    **Resultado en la arquitectura:** 
    Al quitar `{{ source('bronze', 'delta_bruto') }} `, el `.sql` de staging sólo se queda escuchando a `dbt_project.yml` y deja de escuchar a `sources.yml`.  Y aunque la source.yml debería ser el único archivo donde se declara la fuente, ya vimos que saltaba error, por lo que al no usar source() y por ende, ya no escuchar a `sources.yml`, éste deja de ser el inyector del origen de datos (ahora sólo se escucha a`dbt_project.yml`) y pasa a cumplir un rol estrictamente de **documentación y gobernanza del linaje del proyecto** por lo que la arquitectura no resulta redundante.

    Tras aplicar este cambio, el pipeline compila y ejecuta en verde todos los modelos (incluyendo dimensiones y hechos) de forma exitosa:

    ```text
    12:07:51  1 of 5 START sql view model main.delta_silver .................................. [RUN]
    12:07:51  1 of 5 OK created sql view model main.delta_silver ............................. [OK in 0.15s]
    12:07:51  2 of 5 START sql external model main.dim_language .............................. [RUN]
    12:07:51  2 of 5 OK created sql external model main.dim_language ......................... [OK in 0.19s]
    12:07:51  3 of 5 START sql external model main.dim_repositorios .......................... [RUN]
    12:07:51  3 of 5 OK created sql external model main.dim_repositorios ..................... [OK in 0.19s]
    12:07:51  4 of 5 START sql external model main.dim_tiempo ................................ [RUN]
    12:07:51  4 of 5 OK created sql external model main.dim_tiempo ........................... [OK in 0.10s]
    12:07:51  5 of 5 START sql external model main.facts ..................................... [RUN]
    12:07:51  5 of 5 OK created sql external model main.facts ................................ [OK in 0.11s]
    12:07:51  
    12:07:51  Finished running 4 external models, 2 project hooks, 1 view model in 2.78 seconds.
    12:07:52  Completed successfully (PASS=7 TOTAL=7)



# //// AVANCE 6 ///
# Conexión Incremental de DBT con DuckDB y Power BI a través de ODBC
 
Este repositorio contiene la configuración y los pasos necesarios para migrar un flujo de datos analítico desde un almacenamiento basado en archivos `.parquet` sueltos hacia una base de datos local gestionada con **DuckDB**, orquestada por **DBT (Data Build Tool)** y consumida directamente desde **Power BI** utilizando un controlador **ODBC**.
 
---
 
## 🛡️ Arquitectura del Proyecto
 
El objetivo principal es eliminar la persistencia en archivos Parquet independientes y consolidar el modelo de datos (Hechos y Dimensiones) dentro de la memoria interna/archivo persistente de DuckDB. Esto permite que Power BI actúe contra DuckDB como si fuera un catálogo estructurado.
 
## ⚙️ 1. Configuración de DBT
 
### Modificaciones en `dbt_project.yml`
 
Se eliminó la configuración genérica y global de la propiedad `unique_key` y la estrategia de `merge` masiva que se aplicaba de forma uniforme en todo el proyecto. Esto se hizo para evitar colisiones de claves primarias ("mezclar peras con manzanas") entre tablas con naturalezas distintas (por ejemplo, `tiempo` vs `lenguaje`).
 
Las materializaciones se movieron al nivel de cada archivo `.sql` individual.
 
### Configuración por Modelo (`.sql`)
 
Cada archivo de dimensión y hecho debe configurarse de forma independiente para asegurar su persistencia en DuckDB de manera incremental y con su respectiva clave única:
 
```sql
{{ config(
    materialized='incremental',
    unique_key='id_propio_de_la_tabla'
) }}
```
 
## 🔌 2. Configuración del Driver ODBC (DuckDB)
 
Para conectar Power BI con DuckDB se requiere instalar y configurar el driver ODBC oficial de DuckDB.
 
### Pasos de Configuración en el Administrador de Orígenes de Datos ODBC:
 
1. Crear un nuevo DSN de Sistema o de Usuario seleccionando el driver de DuckDB.
2. Nombre de la conexión (DSN): `mi_db`
> ⚠️ **¡CRÍTICO! Solución de Errores de Ruta:**
>
> Por defecto, el driver viene configurado con la palabra `Memory` en la casilla de la base de datos.
> - Debes **desmarcar o borrar por completo** el texto `Memory`. No añadas la ruta a continuación de esa palabra.
> - Introduce la **ruta absoluta completa** hacia tu archivo de base de datos local de DuckDB (ej. `C:\\usuarios\\tu_usuario\\proyecto\\mi_base_de_datos.db`).
> - Guarda y acepta los cambios.
 
> 💡 **Nota de solución de problemas:** Si se deja la opción `Memory`, Power BI intentará buscar una base de datos efímera y vacía en memoria, lo que provocará que las tablas aparezcan en blanco o que la conexión falle de forma persistente debido al almacenamiento en caché de Power BI.
 
## 📊 3. Conexión en Power BI
 
Una vez que el puente ODBC está correctamente configurado apuntando a la ruta absoluta, sigue estos pasos en Power BI Desktop:
 
1. Ve a **Obtener datos** -> **Otros** -> **ODBC**.
2. Selecciona en el desplegable el nombre del DSN configurado: `mi_db`.
3. Cuando Power BI solicite las credenciales de acceso:
   - **Usuario:** `mi_db` *(Introduce el mismo nombre otorgado a la conexión DSN)*.
   - **Contraseña:** Déjala completamente en blanco *(al tratarse de un entorno local de DuckDB, no requiere contraseña)*.
4. Conecta y selecciona las tablas del catálogo de DuckDB para comenzar a modelar.


## 🔄 Solución de Problemas (Cache & Reinstalación)
 
Si realizaste una conexión errónea inicial apuntando a `Memory`, Power BI podría haber cacheado esa ruta vacía. Si no se actualizan los datos tras corregir el DSN:
 
1. Elimina el driver u origen de datos guardado en las configuraciones de Power BI (**Archivo > Opciones y configuración > Configuración de origen de datos**).
2. Asegúrate de limpiar la casilla `Memory` en el panel de control ODBC.
3. Vuelve a mapear la ruta absoluta e intenta la importación de nuevo.

