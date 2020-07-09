package com.dsm.dataframe.dsl


import com.dsm.utils.Constants
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{concat, lit}

import org.apache.spark.sql.functions.split

object gZconversions {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[*]").appName("Dataframe Example").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Constants.ACCESS_KEY)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Constants.SECRET_ACCESS_KEY)

    import sparkSession.implicits._
    val NasaDf = sparkSession.read.option("delimiter", " ")
      .csv("s3n://" + Constants.S3_BUCKET + "/NASA_access_log_Jul95")
      .repartition(10).drop("_c1").drop("_c2")
    /*NasaDf.withColumn("Time_and_Date", concat(col("_c3"),lit(' '),
     col("_c4")))
      .show(10,false) */
   val Final= NasaDf.select($"_c0",concat($"_c3", lit(" "), $"_c4"),$"_c5", concat($"_c6",lit(" "), $"_c7"))
    .show(10,false)



    val priorityLog = NasaDf.groupBy("_c0").count().orderBy($"count".desc)
    priorityLog.select(priorityLog.col("*")).show(1)





    sparkSession.close()

  }
}

