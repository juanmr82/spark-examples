package com.pybsoft.pipelines.pipes

import org.apache.spark.sql.SparkSession

/**
  * Trait to be extended by all Pipes to be run by Pipe Handler
  */
trait Pipe {

  def init(): Unit

  def run(spark: SparkSession): Unit

  def terminate(): Unit

}
