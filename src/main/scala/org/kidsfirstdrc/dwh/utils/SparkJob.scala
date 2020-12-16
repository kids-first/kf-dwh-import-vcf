package org.kidsfirstdrc.dwh.utils

import org.apache.spark.sql.SparkSession

/**
 * A Trait defining a common behaviour for Spark jobs.
 */
trait SparkJob extends App {

  val jobName: String

  /**
   * arguments received as parameters.
   * releaseId is the unique name of the release following re_[0-9]+ format
   * input - the input folder where the data will be read from
   * output - the out put folder where the data will be written
   * runType - one of 'debug', 'normal', 'overwrite'
   */
  val Array(releaseId, input, output, runType) = args

  implicit lazy val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Import $runType for $jobName - $releaseId").getOrCreate()

}
