package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.updates.UpdateVariant
import org.kidsfirstdrc.dwh.utils.Environment

import scala.util.Try

object ImportClinVar extends App {

  val Array(runEnv, update_dependencies) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import Orphanet").getOrCreate()

  val env = Try(Environment.withName(runEnv)).getOrElse(Environment.DEV)

  new ImportClinVarJob(env).run()
  Try {
    if(update_dependencies.toBoolean)
      new UpdateVariant(Environment.PROD).run("s3a://kf-strides-variant-parquet-prd", "s3a://kf-strides-variant-parquet-prd")
  }
}
