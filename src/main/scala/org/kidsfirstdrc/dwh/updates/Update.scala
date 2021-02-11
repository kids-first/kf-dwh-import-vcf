package org.kidsfirstdrc.dwh.updates

import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.conf.Environment
import org.kidsfirstdrc.dwh.conf.Environment._

import scala.util.Try

object Update extends App {
  val Array(source, destination, runEnv) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Update $destination from $source").getOrCreate()

  run(source, destination, runEnv)

  def run(source: String, destination: String, runEnv: String)(implicit spark: SparkSession): Unit = {

    val env = Try(Environment.withName(runEnv)).getOrElse(Environment.DEV)

    (source, destination) match {
        case ("clinvar", "variants")      => new UpdateVariant(Public.clinvar, env).run()
        case ("topmed_bravo", "variants") => new UpdateVariant(Public.topmed_bravo, env).run()
        case _ => throw new IllegalArgumentException(s"No job found for : ($source, $destination)")
    }
  }

}
