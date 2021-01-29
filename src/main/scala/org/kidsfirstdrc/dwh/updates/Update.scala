package org.kidsfirstdrc.dwh.updates

import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.utils.Environment
import org.kidsfirstdrc.dwh.utils.Environment._

import scala.util.Try

object Update extends App {
  val Array(table, runEnv, rootFolder) = args
  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Update table: $table").getOrCreate()

  run(table, runEnv, rootFolder)

  def run(table: String, runEnv: String, rootFolder: String)(implicit spark: SparkSession): Unit = {

    val env = Try(Environment.withName(runEnv)).getOrElse(Environment.DEV)

    val outputFolder = env match {
      case PROD => rootFolder
      case _    => rootFolder + "/tmp"
    }

    table match {
        case "variants" => new UpdateVariant(env).run(rootFolder, outputFolder)
    }
  }

}
