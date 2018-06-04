package com

import org.apache.spark.sql.SparkSession

object InterviewClass {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("yarn").appName("Interview questions").getOrCreate()
    //val count=spark.sparkContext.textFile("my/local/file")
    val lines=scala.io.Source.fromFile("C:\\Users\\PG041051\\Documents\\Zoom\\demo-app\\src\\main\\resources\\wordCountExample").getLines()
    val linesRDD=spark.sparkContext.parallelize(lines.toList).
      flatMap(rec=>rec.split(" ")).
      map(rec=>(rec,1)).reduceByKey(_+_).map(rec=> rec._1 +"\t"+ rec._2)
    //linesRDD.flatMap(rec=>rec)
    linesRDD.collect().foreach(println)


    println("hello added for feature branch")
  }

}
