package org.kidsfirstdrc.dwh.updates

import bio.ferlab.datalake.spark3.config.{Configuration, StorageConf}
import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.conf.Catalog.Public

object Update extends App {
  val Array(source, destination) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Update $destination from $source").getOrCreate()

  implicit val conf: Configuration = Configuration(List(StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd")))


  run(source, destination)

  def run(source: String, destination: String)(implicit spark: SparkSession): Unit = {

    (source, destination) match {
        case ("clinvar", "variants") =>
          new UpdateVariant(Public.clinvar, "variant").run()
          new UpdateVariant(Public.clinvar, "portal").run()

        case ("topmed_bravo", "variants") =>
          new UpdateVariant(Public.topmed_bravo, "variant").run()
          new UpdateVariant(Public.topmed_bravo, "portal").run()

        case _ => throw new IllegalArgumentException(s"No job found for : ($source, $destination)")
    }
  }

}
