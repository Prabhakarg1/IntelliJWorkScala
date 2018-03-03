package com

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SourceDataLoading {
  def main(args: Array[String]): Unit = {
    var sparkSession = SparkSession.builder.master("local[*]").appName("Test App").getOrCreate()
    val config = ConfigFactory.load("application.conf").getConfig("conf")

    // Getting S3 bucket connection details
    val s3Config = config.getConfig("S3")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("s3nAccessKey"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("s3nAccessSecret"))

    // Getting Redshift database connection details
    val redshiftConf = config.getConfig("Redshift")
    val jdbcHostname = redshiftConf.getString("host")
    val jdbcPort = redshiftConf.getString("port")
    val jdbcUsername = redshiftConf.getString("username")
    val jdbcPassword = redshiftConf.getString("password")
    val jdbcDatabase = redshiftConf.getString("database")
    var redshiftJdbcUrl = s"jdbc:redshift://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"

    // Loading all staging tables
    val sourceList = config.getStringList("SourceData").toArray
    for (source <- sourceList) {
      source match {
        case "REGIS" =>
          val stgRegisDF = sparkSession.read
            .option("header","true")
            .option("delimiter" ,"|")
            .csv("s3n://" + s3Config.getString("s3nBucket") + "/KC_Extract_[1-2]_20171009.csv")
          stgRegisDF.withColumn("REGIS_DATE", regexp_replace(col("REGIS_DATE"),"/","-" ))
          println("# of records = " + stgRegisDF.count())
          val stgRegisDFWithTimestampDF = stgRegisDF
//            .withColumn("regis_date", when(col("regis_date").contains("//"), col("regis_date").apply()))
            .withColumn("INS_TS", current_timestamp())
          stgRegisDFWithTimestampDF.show(50)

          stgRegisDFWithTimestampDF.write.format("com.databricks.spark.redshift")
            .option("url", redshiftJdbcUrl)
            .option("tempdir", "s3n://tempdir2")
            .option("dbtable", "STAGING.STG_1CP")
            .option("extracopyoptions", "EMPTYASNULL")
            .option("forward_spark_s3_credentials", "true")
            .mode(SaveMode.Overwrite)
            .save()

        case "OL" =>

          val df = sparkSession.read.
            format("com.springml.spark.sftp").
            option("host", "rscsftp.rmmel.com").
            option("port", 22).
            option("username", "OPQADSP&GUK").
            option("password", "Hansolo0301").
            option("fileType", "csv").
            option("delimiter", "|").
            option("inferSchema", true).
            load("/Datastore/Archive/receipts_delta_GBR_14_10_2017.csv")

          println("Record count = " + df.count())

        case _ =>
          println("None of the source has been mentioned!!")
      }
    }

    sparkSession.close()
    System.exit(0)
  }
}
