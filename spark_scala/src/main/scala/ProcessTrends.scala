// Fase 2: Procesamiento de datos con Apache Spark
// Este es mi archivo de procesamiento donde voy a procesar el archivo bruto

import org.apache.spark.sql.SparkSession

object ProcessTrends {
  def main(args: Array[String]): Unit = {
    println("Iniciando procesamiento de tendencias...")
    // 1. Iniciamos el motor (El Maestro)
    val spark = SparkSession.builder()
      .appName("GitHubTrendsProcessor")
      .master("local[*]") // Esto dice: "Usa todos los núcleos de MI laptop"
      .getOrCreate()

    // 2. Leemos el CSV que creó Python
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true") // Spark adivina si es número o texto cuando lee las primeras líneas. Ten en cuenta que por ello se puede equivocar al inferir.
      .csv("../data/repos_python.csv") // recuerda indicarle en el path que salga de spark_scala y busque FUERA de ella el .csv, si no, no lo va a encontrar.

    // 3. Transformación (Narrow): Filtrar baándome en la columna stars
    val popularRepos = df.filter("stars > 5000")

    // 4. Acción: Mostrar el resultado
    popularRepos.show()

    // 5. Apagar el motor
    spark.stop()
  }
}
