// Fase 2: Procesamiento de datos con Apache Spark
// Este es mi archivo de procesamiento donde voy a procesar el archivo bruto
// Voy a poner logs


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
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

    // 2. Limpieza y Transformación
    val dfClean = df
      .withColumn("stars", col("stars").cast(IntegerType))
      .filter(col("stars").isNotNull)
      .select("name", "stars", "language")

    // 3. Escritura (Ruta de salida que mapearemos a tu Windows)
    // en Delta con MERGE
    val outputPath = "/app/output/bronze/delta_bruto"

    println(s"-> Guardando datos procesados en Delta en: $outputPath")
    
    // Verificamos si la tabla Delta ya existe para hacer el MERGE
    if (DeltaTable.isDeltaTable(spark, outputPath)) {
      val deltaTable = DeltaTable.forPath(spark, outputPath)

      // Ejecutamos el MERGE usando 'name' como clave de negocio
      deltaTable.as("target")
        .merge(
          dfClean.as("updates"),
          "target.name = updates.name"
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