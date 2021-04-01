package org.kidsfirstdrc.dwh.join

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

  run(studyId, releaseId, output, runType, mergeExisting.toBoolean, schema)

  def run(studyId: String, releaseId: String, output: String, runType: String, mergeExisting: Boolean, schema: String)(implicit spark: SparkSession): Unit = {
    val studyIds = studyId.split(",")
    val releaseIdLc = releaseId.toLowerCase()

    spark.sql(s"USE $schema")

    runType match {
      case "variants" => JoinVariants.join(studyIds, releaseIdLc, output, mergeExisting, schema)
      case "consequences" => JoinConsequences.join(studyIds, releaseIdLc, output, mergeExisting, schema)
      case "all" =>
        JoinVariants.join(studyIds, releaseIdLc, output, mergeExisting, schema)
        JoinConsequences.join(studyIds, releaseIdLc, output, mergeExisting, schema)

    }
  }

}
