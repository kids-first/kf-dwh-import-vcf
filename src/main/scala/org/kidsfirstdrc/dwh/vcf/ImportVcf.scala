package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import org.apache.spark.sql.SparkSession

import scala.util.Try

object ImportVcf extends App {

  println(s"Job arguments: ${args.mkString("[", ", ", "]")}")

  val Array(studyId, releaseId, input, runType, biospecimenIdColumn, isPatternOverride, schema) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Import $runType for $studyId - $releaseId").getOrCreate()

  val storage = schema match {
    case "variant" => StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd")
    case "portal" => StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd/portal")
  }

  implicit val conf: Configuration = Configuration(List(
    storage
  ))

  val isPatternOverriden: Boolean = Try(isPatternOverride.toBoolean).getOrElse(false)

  run(studyId, releaseId, input, runType, biospecimenIdColumn, isPatternOverriden, schema)

  def run(studyId: String, releaseId: String, input: String,
          runType: String = "all", biospecimenIdColumn: String = "biospecimen_id", isPatternOverriden: Boolean = false,
          schema: String = "variant")
         (implicit spark: SparkSession, conf: Configuration): Unit = {
    spark.sql(s"USE $schema")

    runType match {
      case "occurrences" => new Occurrences(studyId, releaseId, input, biospecimenIdColumn, isPatternOverriden).run()
      case "variants" => new Variants(studyId, releaseId, schema).run()
      case "consequences" => new Consequences(studyId, releaseId, input).run()
      case "all" =>
        new Occurrences(studyId, releaseId, input, biospecimenIdColumn, isPatternOverriden).run()
        new Variants(studyId, releaseId, schema).run()
        new Consequences(studyId, releaseId, input).run()

    }
  }

}

