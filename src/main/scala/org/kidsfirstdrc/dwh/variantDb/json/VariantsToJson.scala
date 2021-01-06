package org.kidsfirstdrc.dwh.variantDb.json

import org.apache.spark.sql.SparkSession

object VariantsToJson  extends App {

  val Array(releaseId, input, output) = args

  implicit lazy val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Import VariantsToJson - $releaseId").getOrCreate()

  val job = new VariantsToJsonJob(releaseId)

  val sources = job.extract(input)
  val destination = job.transform(sources)
  job.load(destination, output)

}