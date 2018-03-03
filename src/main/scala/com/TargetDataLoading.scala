package com
import util.control.Breaks._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

object TargetDataLoading {
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
    val targetConf = config.getConfig("TargetData")
    val targetTableList = targetConf.getStringList("TableList").toArray
    for (targetTable <- targetTableList) {
      targetTable match {
        case "REGIS_DIM" =>
          val srcTableName = targetConf.getConfig(targetTable.toString).getStringList("sourceTable").get(0).replace(".", "_")
          val stg1CpDf = sparkSession.read.format("com.databricks.spark.redshift")
            .option("url", redshiftJdbcUrl)
            .option("tempdir", "s3n://tempdir2")
            .option("dbtable", "staging.stg_1cp")
            .option("tempformat", "CSV")
            .option("forward_spark_s3_credentials", "true")
            .load()
          stg1CpDf.createOrReplaceTempView(srcTableName)
          //stg1CpDf.show(5)
          val loadingQuery = targetConf.getConfig(targetTable.toString).getString("loadingQuery")
          val a=sparkSession.sparkContext.broadcast(loadingQuery)
          sparkSession.sql(a.value).repartition(8)
            .write
            .format("com.databricks.spark.redshift")
            .option("url", redshiftJdbcUrl)
            .option("tempdir", "s3n://tempdir2")
            .option("dbtable", targetConf.getConfig(targetTable.toString).getString("tableName"))
            .option("tempformat", "CSV") // Keeping datatype as avro as there is some issue with CSV and
            .option("extracopyoptions", "EMPTYASNULL")
            .option("forward_spark_s3_credentials", "true")
            .mode(SaveMode.Append)
            .save()

          println()
        case _ =>
          println()
      }
    }

    sparkSession.close()
    System.exit(0)
  }
}
