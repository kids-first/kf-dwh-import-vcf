package org.kidsfirstdrc.dwh.variantDb.json

import org.apache.spark.sql.SparkSession

object VariantsToJson extends App {

  val Array(releaseId) = args

  implicit lazy val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Export variant_index - $releaseId").getOrCreate()

  new VariantsToJsonJob(releaseId).run()

}