// Fase 2: Procesamiento de datos con Apache Spark
// Este es mi archivo de procesamiento donde voy a procesar el archivo bruto
// Voy a poner logs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.log4j.{Level, Logger, PatternLayout, FileAppender, ConsoleAppender}

object ProcessTrends {
  // 1. Definimos el Logger global para el objeto
  val logger: Logger = Logger.getLogger("GitHubTrendsProcessor")

  def main(args: Array[String]): Unit = {

    // --- CONFIGURACIÓN DEL VIGILANTE (LOGS) ---
    val layout = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}: %m%n")
    
    // Log en Consola (para verlo ahora)
    logger.addAppender(new ConsoleAppender(layout))
    
    // Log en Archivo (para Auditoría del Módulo 2)
    try {
      val fileAppender = new FileAppender(layout, "logs/pipeline_debug.log", true)
      logger.addAppender(fileAppender)
    } catch {
      case e: Exception => println("No se pudo crear el archivo de log: " + e.getMessage)
    }

    logger.setLevel(Level.INFO)
    Logger.getLogger("org").setLevel(Level.DEBUG) // Silenciamos el ruido interno de Spark
    // ------------------------------------------

    logger.info(s"HADOOP_HOME detectado: ${System.getenv("HADOOP_HOME")}")
    logger.info("Iniciando procesamiento de tendencias...")

    val spark = SparkSession.builder()
      .appName("GitHubTrendsProcessor")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", new java.io.File("spark-warehouse").getAbsolutePath)
      .config("hive.exec.scratchdir", new java.io.File("target/hive").getAbsolutePath)
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .getOrCreate()

    try {
      logger.info("Leyendo CSV de entrada...")
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("../data/repos_python.csv")

      val nomCol_clean = Seq("stars")
      
      logger.info(s"Aplicando transformación foldLeft sobre columnas: ${nomCol_clean.mkString(", ")}")
      val df_clean = nomCol_clean.foldLeft(df) { (tempDf, colName) => 
        tempDf.withColumn(colName, col(colName).cast(IntegerType))
      }

      logger.info("Iniciando limpieza de valores nulos en columna 'stars'...")
      val df_final = df_clean.na.drop(Seq("stars"))

      val totalRows = df.count()
      val cleanedRows = df_final.count()
      
      logger.info(s"Métricas de calidad: Originales [$totalRows] | Limpias [$cleanedRows]")

      // 6. Intento de Guardado con Trazabilidad Total
      logger.info("Iniciando fase de guardado en formato Parquet...")
      
      try {
        // Forzamos la ejecución de la acción antes de escribir para asegurar que el plan es válido
        df_final.coalesce(1)
          .write
          .mode("overwrite")
          .option("compression", "none")
          .parquet("C:/repos_parquet_test") 
        
        logger.info("!!! ÉXITO: El archivo Parquet se ha generado en C:/repos_parquet_test !!!")
      } catch {
        case e: Exception => 
          logger.error(s"### FALLO EN ESCRITURA: La ruta existe pero el archivo no se completó.")
          logger.error(s"Causa técnica: ${e.getMessage}")
          // Esto es lo que pide el Módulo 2: volcar el error completo al log
          logger.debug("Stacktrace completo:", e) 
      }

      // 7. Mostrar resultados
      df_final.show(20, false)

    } catch {
      case e: Exception => 
        logger.fatal(s"ERROR CRÍTICO EN EL FLUJO: ${e.getMessage}")
    } finally {
      logger.info("Apagando motor Spark. Fin del diario de ejecución.")
      spark.stop()
    }
  }
}