package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.utils.Environment
import org.kidsfirstdrc.dwh.utils.Environment.PROD

import scala.util.Try

object ImportOrphanet extends App {

  val Array(input, output, runEnv) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import Orphanet").getOrCreate()

  val env = Try(Environment.withName(runEnv)).getOrElse(Environment.DEV)

  val outputFolder = env match {
    case PROD => output
    case _    => output + "/tmp"
  }

  new ImportOrphanetJob(env).run(input, outputFolder)
}
