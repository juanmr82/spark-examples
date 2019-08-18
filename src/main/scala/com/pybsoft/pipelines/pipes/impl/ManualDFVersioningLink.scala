package com.pybsoft.pipelines.pipes.impl

import com.pybsoft.pipelines.pipes.Pipe
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lead
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions.rand

class ManualDFVersioningLink( val debugFlag:Boolean = false) extends Pipe  {

  var initLogLevel: Level = Logger.getLogger("org.apache").getLevel()


  override def init(): Unit = {

  }


  override def run(spark:SparkSession): Unit = {

    if(debugFlag) System.out.println("-->Starting Pipeline DF Version Link")


    import spark.implicits._

    val initialDF = Seq(

      //Rows for Object 1
      ("__abc1", "1.4.0", "uuid_6", "SOURCE_TYPE_A", "2019-04-01 15:56:45"),
      ("__abc1", "1.0.2", "uuid_3", "SOURCE_TYPE_A", "2019-04-01 12:50:45"),
      ("__abc1", "1.3.0", "uuid_5", "SOURCE_TYPE_A", "2019-04-01 15:50:45"),
      ("__abc1", "1.0.1", "uuid_2", "SOURCE_TYPE_A", "2019-04-01 11:55:45"),
      ("__abc1", "1.4.1", "uuid_7", "SOURCE_TYPE_A", "2019-04-03 16:50:45"),
      ("__abc1", "1.0.0", "uuid_1", "SOURCE_TYPE_A", "2019-04-01 11:50:45"),
      ("__abc1", "1.5.0", "uuid_8", "SOURCE_TYPE_A", "2019-04-04 11:50:45"),
      ("__abc1", "1.2.0", "uuid_4", "SOURCE_TYPE_A", "2019-04-01 13:55:45"),

      //Rows for Object 2
      ("__abc2", "1.2.0", "uuid_11", "SOURCE_TYPE_A", "2019-02-02 10:50:45"),
      ("__abc2", "1.3.0", "uuid_13", "SOURCE_TYPE_A", "2019-02-03 16:50:45"),
      ("__abc2", "1.0.1", "uuid_10", "SOURCE_TYPE_A", "2019-02-01 16:50:30"),
      ("__abc2", "1.2.1", "uuid_12", "SOURCE_TYPE_A", "2019-02-02 11:55:45"),
      ("__abc2", "1.0.0", "uuid_9", "SOURCE_TYPE_A", "2019-02-01 15:50:45"),

      //Rows for Object 3
      ("__abc3", "1.0.3", "uuid_17", "SOURCE_TYPE_B", "2019-06-04 11:55:45"),
      ("__abc3", "1.0.0", "uuid_14", "SOURCE_TYPE_B", "2019-06-01 10:50:45"),
      ("__abc3", "1.0.2", "uuid_16", "SOURCE_TYPE_B", "2019-06-03 12:50:45"),
      ("__abc3", "1.0.4", "uuid_18", "SOURCE_TYPE_B", "2019-06-05 15:50:45"),
      ("__abc3", "1.0.1", "uuid_15", "SOURCE_TYPE_B", "2019-06-02 16:50:45"),

      //Rows for Object 4, which has same ID as Object 1 but in other system
      ("__abc1", "1.0.3", "uuid_21", "SOURCE_TYPE_B", "2019-06-04 11:55:45"),
      ("__abc1", "1.0.0", "uuid_18", "SOURCE_TYPE_B", "2019-06-01 10:50:45"),
      ("__abc1", "1.0.2", "uuid_20", "SOURCE_TYPE_B", "2019-06-03 12:50:45"),
      ("__abc1", "1.0.4", "uuid_22", "SOURCE_TYPE_B", "2019-06-05 15:50:45"),
      ("__abc1", "1.0.1", "uuid_19", "SOURCE_TYPE_B", "2019-06-02 16:50:45"),

      //Branch of Object 5,which is a branch of Object 3

      ("__abc3", "1.0.3_branch", "uuid_24", "SOURCE_TYPE_B", "2019-06-03 12:55:48"),
      ("__abc3", "1.0.2_branch", "uuid_23", "SOURCE_TYPE_B", "2019-06-03 12:50:45"),
      ("__abc3", "1.0.5_branch", "uuid_26", "SOURCE_TYPE_B", "2019-06-04 17:24:28"),
      ("__abc3", "1.0.4_branch", "uuid_25", "SOURCE_TYPE_B", "2019-06-04 10:56:48")

    )
      .toDF("fragment", "version", "uuid", "system", "date")
      //Shuffle a bit more
      .orderBy(rand())


    if(debugFlag){
      System.out.println("-->Initial Dataframe")
      initialDF.show(30)
    }

    /**
      * Create a windonwing function using as references
      *   - Rows of column "fragment" with the same value
      *   - Rows of column "system" with the same value
      *   - Rows of column "version", differenciating if they contain the word "branch" or not
      *  After grouping, each window will be sorted descending order using column "date"
      */
    val window = Window.partitionBy('fragment,'system,'version contains "branch").orderBy('date desc)

    /**
      * lead column "uuid" on each  window above specified. This causes a shift uppwards of the row values
      */
    val destUUID = lead(initialDF("uuid"),1).over(window)

    /**
      * Attach Lead column to the dataframe. This will assign to each row the uuid of the following row. This is how the
      * link _previousVersion is stablished.
      * Drop the columns which wont be used during export to Neo4J
      */
    val resultDF = initialDF.withColumn("dest_uuid",destUUID)
      .filter('dest_uuid isNotNull)
      .drop("fragment","version","system","date","timestamp")
      .cache()

    if(debugFlag){
      System.out.println("-->Dataframe after applying Windowing function")
      initialDF.withColumn("dest_uuid",destUUID).show(30)

      System.out.println("-->Filtering Null values and removing columns before export")
    }


    /**
      * DO NOT USE IN PRODUCTION.
      * Replace it by NEO4J Export function. USed to trigger the execution of Spark code
      */
    resultDF.show(30)

    //Unpersist all
    resultDF.unpersist()


  }

  override def terminate(): Unit = {

  }


}

