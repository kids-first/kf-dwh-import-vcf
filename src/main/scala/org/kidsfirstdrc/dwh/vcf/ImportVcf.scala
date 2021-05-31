package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.spark3.config.{Configuration, StorageConf}
import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.conf.Catalog

object ImportVcf extends App {

  println(s"Job arguments: ${args.mkString("[", ", ", "]")}")

  val Array(
    studyId,
    releaseId,
    folder,
    runType,
    biospecimenIdColumn,
    cgp_pattern,
    post_cgp_pattern,
    schema
  ) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config(
      "hive.metastore.client.factory.class",
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )
    .config("spark.sql.broadcastTimeout", "3600")
    .enableHiveSupport()
    .appName(s"Import $runType for $studyId - $releaseId")
    .getOrCreate()

  val storage = schema match {
    case "variant" => StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd")
    case "portal" =>
      StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd/portal")
  }

  implicit val conf: Configuration = Configuration(
    List(storage),
    sources = Catalog.sources.toList
  )

  run(
    studyId,
    releaseId,
    s"s3a://kf-study-us-east-1-prd-${studyId.replace("_", "-").toLowerCase}/$folder",
    runType,
    biospecimenIdColumn,
    cgp_pattern,
    post_cgp_pattern,
    schema
  )

  def run(
      studyId: String,
      releaseId: String,
      input: String,
      runType: String = "all",
      biospecimenIdColumn: String = "biospecimen_id",
      cgp_pattern: String = ".CGP.filtered.deNovo.vep.vcf.gz",
      post_cgp_pattern: String = ".postCGP.filtered.deNovo.vep.vcf.gz",
      schema: String = "variant"
  )(implicit spark: SparkSession, conf: Configuration): Unit = {
    spark.sql(s"USE $schema")

    runType match {
      case "occurrences_family" =>
        new OccurrencesFamily(
          studyId,
          releaseId,
          input,
          biospecimenIdColumn,
          cgp_pattern,
          post_cgp_pattern
        ).run()
      case "occurrences" => new Occurrences(studyId, releaseId).run()
      case "variants"    => new Variants(studyId, releaseId, schema).run()
      case "consequences" =>
        new Consequences(studyId, releaseId, input, cgp_pattern, post_cgp_pattern).run()
      case "all" =>
        new OccurrencesFamily(
          studyId,
          releaseId,
          input,
          biospecimenIdColumn,
          cgp_pattern,
          post_cgp_pattern
        ).run()
        new Occurrences(studyId, releaseId).run()
        new Variants(studyId, releaseId, schema).run()
        new Consequences(studyId, releaseId, input, cgp_pattern, post_cgp_pattern).run()

    }
  }

}
