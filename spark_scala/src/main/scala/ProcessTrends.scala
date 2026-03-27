// Fase 2: Procesamiento de datos con Apache Spark
// Este es mi archivo de procesamiento donde voy a procesar el archivo bruto
// Voy a poner logs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.log4j.{Level, Logger, PatternLayout, FileAppender, ConsoleAppender}


import org.apache.log4j.LogManager 

object ProcessTrends {
  // 1. Definimos el Logger global para el objeto
  val logger: Logger = Logger.getLogger("GitHubTrendsProcessor")
  

  def main(args: Array[String]): Unit = {
    System.setProperty("java.library.path", "C:\\hadoop\\bin")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    // Crea estas carpetas a mano en tu proyecto si no existen: "temp_spark" y "logs"
    val baseDir = new java.io.File("..").getAbsolutePath // Ruta de tu proyecto
    // --- CONFIGURACIÓN DEL VIGILANTE (LOGS) ---
    val layout = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}: %m%n")
    
    // Log en Consola (para verlo ahora)
    logger.addAppender(new ConsoleAppender(layout))
    
    // Log en Archivo (para Auditoría del Módulo 2)
    try {
      val fileAppender = new FileAppender(layout, "../logs/pipeline_debug.log", true)
      fileAppender.setImmediateFlush(true)
      logger.addAppender(fileAppender)
    } catch {
      case e: Exception => println("No se pudo crear el archivo de log: " + e.getMessage)
    }

    logger.setLevel(Level.INFO)
    Logger.getLogger("org").setLevel(Level.DEBUG) // Silenciamos el ruido interno de Spark
    // ------------------------------------------

    logger.info(s"Base directory del proyecto: $baseDir")
    logger.info(s"HADOOP_HOME detectado: ${System.getenv("HADOOP_HOME")}")
    logger.info("Iniciando procesamiento de tendencias...")

    System.setProperty("java.library.path", "C:\\hadoop\\bin")
    // 1. Forzar el uso del sistema de archivos local básico (evita el uso de NativeIO)
    System.setProperty("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    // 2. Desactivar el marcador de éxito (opcional, ayuda a evitar errores de escritura finales)
    System.setProperty("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    val spark = SparkSession.builder()
      .appName("GitHubTrendsProcessor")
      .master("local[*]")
// REDIRIGIMOS TODO A TU CARPETA DE PROYECTO
      .config("spark.sql.warehouse.dir", s"$baseDir/spark-warehouse")
      .config("hive.exec.scratchdir", s"$baseDir/temp_spark/hive")
      .config("spark.hadoop.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$baseDir/temp_spark/metastore_db;create=true")
      // Forzamos el guardado compatible con Windows
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
      .config("spark.speculation", "false")
      .getOrCreate()

    try {
      logger.info("Leyendo CSV de entrada...")
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("../data/repos_python.csv") //C:/010_/Trends-Watcher/data/repos_python.csv

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
      logger.info("Iniciando fase de guardado en formato CSV...")
      
      try {
        logger.info("Verificando datos antes de escribir...")
        df_final.show(5) // Si esto funciona, los datos están vivos en la RAM
        
        val outputPath = s"$baseDir/resultado_debug_csv"
        logger.info(s"Intentando escribir CSV a la ruta absoluta: $outputPath")
        df_final.coalesce(1)
          .write
          .mode("overwrite")
          .option("header", "true")
          .csv(System.getProperty("user.home") + "/Desktop/test_spark")
        // Forzamos la ejecución de la acción antes de escribir para asegurar que el plan es válido
        df_final.coalesce(1)
          .write
          .mode("overwrite")
          .option("header", "true")
          .csv(outputPath)
        
        logger.info("!!! ÉXITO: El archivo CSV se ha generado !!!")
      } catch {
        case e: Exception => 
          logger.error(s"### FALLO EN ESCRITURA CSV:")
          logger.error(s"Causa técnica: ${e.getMessage}")
          // Esto es lo que pide el Módulo 2: volcar el error completo al log
          e.printStackTrace()
          e.getStackTrace.foreach(line => logger.error(line.toString))
          LogManager.shutdown()
      }

      // Intentar también Parquet
      try {
        val parquetPath = s"$baseDir/resultado_debug_parquet"
        logger.info(s"Intentando escribir Parquet a la ruta absoluta: $parquetPath")
        df_final.coalesce(1)
          .write
          .mode("overwrite")
          .parquet(parquetPath)
        
        logger.info("!!! ÉXITO: El archivo Parquet se ha generado !!!")
      } catch {
        case e: Exception => 
          logger.error(s"### FALLO EN ESCRITURA PARQUET:")
          logger.error(s"Causa técnica: ${e.getMessage}")
          e.printStackTrace()
          e.getStackTrace.foreach(line => logger.error(line.toString))
          LogManager.shutdown()
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