// Fase 2: Procesamiento de datos con Apache Spark
// Este es mi archivo de procesamiento donde voy a procesar el archivo bruto
// Voy a poner logs


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object ProcessTrends {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("GitHubTrendsProcessor")
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
    val outputPath = "/app/output/resultado_parquet"

    println(s"-> Guardando datos procesados en: $outputPath")
    
    dfClean.write
      .mode("overwrite")
      .parquet(outputPath)

    println("✅ ¡Éxito! El archivo Parquet ha sido generado.")
    
    spark.stop()
  }
}