name := "GitHubTrends"

version := "0.1"

scalaVersion := "2.12.18" // Versión estable compatible con la mayoría de Spark

val sparkVersion = "3.5.0" // La versión más común hoy en día

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion
)

// Tuve que añadir estos dos apartados porque me saltaba un error
// 1. Forzar a que el programa corra en un proceso separado
run / fork := true

// 2. Darle los permisos de acceso a los módulos de Java 17
run / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
)

// Explicación: El cambio en Java 17: En versiones anteriores de Java, 
// esto estaba permitido. Pero en Java 17, Oracle cerró la puerta. Si tu programa intenta tocar esa memoria sin el permiso explícito (--add-opens), la JVM lo mata inmediatamente por seguridad. Es por eso que otros programas te funcionan, pero Spark (que es más invasivo con el hardware) no.



