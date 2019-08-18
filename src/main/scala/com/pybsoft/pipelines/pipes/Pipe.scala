package com.pybsoft.pipelines.pipes

import org.apache.spark.sql.SparkSession

trait Pipe {

  def init(): Unit

  def run(spark: SparkSession): Unit

  def terminate(): Unit

}
