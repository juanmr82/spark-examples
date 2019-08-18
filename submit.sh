#!/usr/bin/env bash


# SPARK_HOME variable must be set
source ~/.profile


$SPARK_HOME/bin/spark-submit \
  --class com.pybsoft.pipelines.PipeHandler\
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf "spark.executor.extraJavaOptions=-XX:+UseCompressedOops" \
  --conf "spark.driver.extraJavaOptions=-XX:+UseCompressedOops" \
  --master local[*] \
  ./target/uber-spark-pipelines-1.0-SNAPSHOT.jar d -l OFF\

