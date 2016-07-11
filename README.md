#Spark SQL + Crate.IO

Simple spark job that generates test data and ingests into `Crate` database to check how it performs with big ingestion rates.

### How to build

`sbt assembly`

### How to run

- create database table or just allow script crate it with hardcoded table options

```
CREATE TABLE IF NOT EXISTS spark_limits_test (
  time TIMESTAMP,
  label STRING,
  value DOUBLE,
  comment STRING
)
CLUSTERED INTO 40 shards with (number_of_replicas = 1, refresh_interval=10000)
```

- run `spark-submit`

```
spark-submit --master spark://diver.cluster:7077 --deploy-mode cluster --total-executor-cores 10 --class Main hdfs:///spark-crate-playground-assembly-0.0.jar data-01.cluster:4300,data-02.cluster:4300/doc 100000 60
```
