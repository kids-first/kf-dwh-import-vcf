package org.kidsfirstdrc.dwh.join

import bio.ferlab.datalake.commons.config.{Configuration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.conf.Catalog

object Join extends App {
  val Array(studyId, releaseId, runType, mergeExisting, schema) = args
  implicit val spark: SparkSession = SparkSession.builder
    .config(
      "hive.metastore.client.factory.class",
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )
    .enableHiveSupport()
    .appName(s"Join $runType for $studyId - $releaseId - $schema")
    .getOrCreate()

  val output = schema match {
    case "variant" => "s3a://kf-strides-variant-parquet-prd"
    case _  => s"s3a://kf-strides-variant-parquet-prd/$schema"
  }

  implicit val conf: Configuration = Configuration(
    List(StorageConf("kf-strides-variant", output, S3)),
    sources = Catalog.sources.toList
  )

  run(studyId, releaseId, runType, mergeExisting.toBoolean, schema)

  def run(
      studyId: String,
      releaseId: String,
      runType: String,
      mergeExisting: Boolean,
      schema: String
  )(implicit spark: SparkSession): Unit = {
    val studyIds    = studyId.split(",")
    val releaseIdLc = releaseId.toLowerCase()

    spark.sql(s"USE $schema")

    runType match {
      case "variants" => new JoinVariants(studyIds, releaseIdLc, mergeExisting, schema).run()
      case "consequences" =>
        new JoinConsequences(studyIds, releaseIdLc, mergeExisting, schema).run()
      case "all" =>
        new JoinVariants(studyIds, releaseIdLc, mergeExisting, schema).run()
        new JoinConsequences(studyIds, releaseIdLc, mergeExisting, schema).run()

    }
  }

}
