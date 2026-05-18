# Dockerfile para Spark con Scala 2.12
# Imagen base: Apache Spark 3.5.0 (incluye Scala 2.12 por defecto)
# 1. Base de Java 11 (la más estable para Scala 2.12)
FROM amazoncorretto:11

# 2. Instalamos Spark 3.5.0 manualmente para que no haya archivos corruptos
RUN yum install -y tar gzip
RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    && tar -xzf spark-3.5.0-bin-hadoop3.tgz \
    && mv spark-3.5.0-bin-hadoop3 /opt/spark \
    && rm spark-3.5.0-bin-hadoop3.tgz

# 3. Variables de entorno
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
WORKDIR /app

# 4. Copiamos el JAR
#Recuerda que el .jar debe estar ya creado, está en la carpeta oculta target
COPY target/scala-2.12/githubtrends_2.12-0.1.jar /app/app.jar

# 5. Ejecución directa (sin scripts intermedios)
CMD ["spark-submit", "--class", "ProcessTrends", "/app/app.jar"]


#Para CONSTRUIR la imagen:
# la imagen se va a llamar spark-docker-app
# (.venv) PS C:\010_\Trends-Watcher\spark_scala> docker build -t spark-docker-app .  

# Si vas a correr el contenedor, y el Dockerfile no está en la raiz sino en una carpeta, muévete
# a ese carpeta primero, y de ahí ejecutas:
# (.venv) PS C:\010_\Trends-Watcher> cd C:\010_\Trends-Watcher\spark_scala                                     
# (.venv) PS C:\010_\Trends-Watcher\spark_scala> docker run --rm `  -v "${PWD}/../data:/app/data" `  -v "${PWD}/../output:/app/output" `  spark-docker-app

