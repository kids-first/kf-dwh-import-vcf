package org.kidsfirstdrc.dwh.jobs

import org.apache.spark.sql.SparkSession

/**
 * A Trait defining a common behaviour for Spark jobs.
 */
trait SparkJob extends App {

  /**
   * arguments received as parameters.
   * releaseId is the unique name of the release following re_[0-9]+ format
   * input - the input folder where the data will be read from
   * output - the out put folder where the data will be written
   * runType - one of 'debug', 'normal', 'overwrite'
   */
  val Array(jobName, releaseId, input, output, runType) = args

  implicit lazy val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Import $runType for $jobName - $releaseId").getOrCreate()

  def run(etl: MultiSourceEtlJob): Unit = {
    runType match {
      case "debug" =>
        val sources = etl.extract(input)
        etl.transform(sources)

      case "normal" =>
        val sources = etl.extract(input)
        val target = etl.transform(sources)
        etl.load(target, output)
    }

  }
}
