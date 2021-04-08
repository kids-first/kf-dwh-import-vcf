package org.kidsfirstdrc.dwh.join

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import org.apache.spark.sql.SparkSession

object Join extends App {
  val Array(studyId, releaseId, runType, mergeExisting, schema) = args
  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Join $runType for $studyId - $releaseId").getOrCreate()

  val output = schema match {
    case "variant" => "s3a://kf-strides-variant-parquet-prd"
    case "portal" => "s3a://kf-strides-variant-parquet-prd/portal"
  }

  implicit val conf: Configuration = Configuration(List(
    StorageConf("kf-strides-variant", output)
  ))

  run(studyId, releaseId, runType, mergeExisting.toBoolean, schema)

  def run(studyId: String, releaseId: String, runType: String, mergeExisting: Boolean, schema: String)(implicit spark: SparkSession): Unit = {
    val studyIds = studyId.split(",")
    val releaseIdLc = releaseId.toLowerCase()

    spark.sql(s"USE $schema")

    runType match {
      case "variants" => new JoinVariants(studyIds, releaseIdLc, mergeExisting, schema).run()
      case "consequences" => new JoinConsequences(studyIds, releaseIdLc, mergeExisting, schema).run()
      case "all" =>
        new JoinVariants(studyIds, releaseIdLc, mergeExisting, schema).run()
        new JoinConsequences(studyIds, releaseIdLc, mergeExisting, schema).run()

    }
  }

}
