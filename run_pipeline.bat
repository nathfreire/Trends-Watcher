@echo off
echo Installing Python dependencies...
pip install -r requirements.txt

echo Running Python script to fetch data...
python scripts_python\extract_api.py

echo Building Scala project...
cd spark_scala
sbt package
cd ..

echo Building Docker image...
docker build -t trends-watcher spark_scala

echo Running Docker container...
docker run -v %cd%\data:/app/data -v %cd%\output:/app/output trends-watcher

echo Pipeline completed!