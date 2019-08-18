
@ECHO OFF

IF "%SPARK_HOME%"=="" ECHO SPARK_HOME is not defined. Finishing script
IF "%SPARK_HOME%"=="" EXIT


%SPARK_HOME%/bin/spark-submit \
  --class com.pybsoft.pipelines.PipeHandler\
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf "spark.executor.extraJavaOptions=-XX:+UseCompressedOops" \
  --conf "spark.driver.extraJavaOptions=-XX:+UseCompressedOops" \
  --master local[*] \
  ./target/uber-spark-pipelines-1.0-SNAPSHOT.jar -d -l OFF\