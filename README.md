# Apache-Log-Parser
This is the Apache Log Parsing project in CloudxLab for the Big Data with Hadoop & Spark certificate. This project aims at processing raw web logs to extract desired information. The raw web logs are stored in HDFS lab in CloudxLab, originated from NASA Kennedy Space Center WWW server. Below are the processes of compiling the code and running the log parser using Spark.

## Compiling the target
```scala
sbt package
```

## Run unit tests
```scala
sbt test
```
## Run the log parser on the logs stored in HDFS with scala
```scala
// Use Spark 2
export SPARK_MAJOR_VERSION=2
spark-submit target/scala-2.10/apache-log-parsing_2.10-0.0.1.jar logparsing.Entrypoint /data/spark/project/NASA_access_log_Aug95.gz
```
