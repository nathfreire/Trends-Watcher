// Fase 2: Procesamiento de datos con Apache Spark
// Este es mi archivo de procesamiento donde voy a procesar el archivo bruto
// Voy a poner logs


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables._ // Importamos las librerías de Delta Lake

object ProcessTrends {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("GitHubTrendsProcessor")

      // Configuraciones recomendadas para asegurar el soporte de Delta en Spark
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
 
      // No ponemos .master("local") porque Docker/Spark-Submit ya lo gestionan
      .getOrCreate()

    import spark.implicits._

    // 1. Carga de datos (Ruta interna del contenedor)
    val inputPath = "/app/data/repos_python.csv"
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    // 2. Aseguro tipado correcto
    val dfTyped = df
      .withColumn("id", col("id").cast(LongType))                 // Los IDs de GitHub pueden ser muy grandes
      .withColumn("name", col("name").cast(StringType))
      .withColumn("owner_login", col("owner_login").cast(StringType))
      .withColumn("description", col("description").cast(StringType))
      
      // Parseo de fechas (ISO 8601 string a Timestamp)
      .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .withColumn("updated_at", to_timestamp(col("updated_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      
      // Métricas numéricas e indicadores de tamaño
      .withColumn("size", col("size").cast(LongType))
      .withColumn("stargazers_count", col("stargazers_count").cast(IntegerType))
      .withColumn("watchers_count", col("watchers_count").cast(IntegerType))
      .withColumn("forks_count", col("forks_count").cast(IntegerType))
      .withColumn("open_issues_count", col("open_issues_count").cast(IntegerType))
      
      // Texto y metadatos
      .withColumn("language", col("language").cast(StringType))
      .withColumn("license_name", col("license_name").cast(StringType))
      .withColumn("topics", col("topics").cast(StringType))
    

    // 3. Aplicamos la limpieza básica de registros correlativos
    val dfClean = dfTyped
    // Regla crítica: Un repositorio sin ID o sin Nombre es basura o un registro corrupto
      .filter(col("id").isNotNull && col("name").isNotNull)
      
      // Limpieza de texto: Si la descripción viene vacía ("") la transformamos en un NULL real
      .withColumn("description", when(trim(col("description")) === "", null).otherwise(col("description")))
      
      // Limpieza de nulos por defecto en métricas (evitamos problemas al sumar o promediar en dbt)
      .withColumn("stargazers_count", coalesce(col("stargazers_count"), lit(0)))
      .withColumn("watchers_count", coalesce(col("watchers_count"), lit(0)))
      .withColumn("forks_count", coalesce(col("forks_count"), lit(0)))
      .withColumn("open_issues_count", coalesce(col("open_issues_count"), lit(0)))  
  

    println(s"-> Tipado, limpieza en Spark")

    // 4. Escritura (Ruta de salida que mapearemos a tu Windows)
    // en Delta con MERGE
    val outputPath = "/app/output/bronze/delta_bruto"

    println(s"-> Guardando datos procesados en Delta en: $outputPath")
    
    // Verificamos si la tabla Delta ya existe para hacer el MERGE
    if (DeltaTable.isDeltaTable(spark, outputPath)) {
      val deltaTable = DeltaTable.forPath(spark, outputPath)

      // Ejecutamos el MERGE usando 'id' como clave de negocio
      deltaTable.as("target")
        .merge(
          dfClean.as("updates"),
          "target.id = updates.id"
        )
        .whenMatched().updateAll() // Si el repo ya existe, actualiza sus estrellas/idioma
        .whenNotMatched().insertAll() // Si es nuevo, lo inserta
        .execute()

      println("✅ ¡Éxito! Datos integrados mediante MERGE en la tabla Delta.")
    } else {
      // Si la tabla no existe (primera ejecución), la creamos escribiendo el dataframe directamente
      dfClean.write
        .format("delta")
        .mode("overwrite")
        .save(outputPath)

      println("🆕 Tabla Delta original no encontrada. Se ha creado y cargado por primera vez Delta.")
    }
    
    
    spark.stop()
  }
}