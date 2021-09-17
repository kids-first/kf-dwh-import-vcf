package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.spark3.config.{Configuration, StorageConf}
import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.conf.Catalog

object ImportVcf extends App {

  println(s"Job arguments: ${args.mkString("[", ", ", "]")}")

  val Array(
    studyId,
    releaseId,
    _,
    runType,
    biospecimenIdColumn,
    cgp_pattern,
    post_cgp_pattern,
    schema,
    refGenome
  ) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config(
      "hive.metastore.client.factory.class",
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )
    .config("spark.sql.broadcastTimeout", "3600")
    .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
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
    runType,
    biospecimenIdColumn,
    cgp_pattern,
    post_cgp_pattern,
    schema,
    Some(refGenome)
  )

  def run(studyId: String,
          releaseId: String,
          runType: String = "all",
          biospecimenIdColumn: String = "biospecimen_id",
          cgp_pattern: String = ".CGP.filtered.deNovo.vep.vcf.gz",
          post_cgp_pattern: String = ".postCGP.filtered.deNovo.vep.vcf.gz",
          schema: String = "variant",
          refGenome: Option[String] = None
         )(implicit spark: SparkSession, conf: Configuration): Unit = {

    spark.sql(s"USE $schema")

    runType match {
      case "occurrences_family" =>
        new OccurrencesFamily(studyId, releaseId, biospecimenIdColumn, cgp_pattern, post_cgp_pattern, refGenome).run()
      case "occurrences" => new Occurrences(studyId, releaseId).run()
      case "variants"    => new Variants(studyId, releaseId, schema).run()
      case "consequences" =>
        new Consequences(studyId, releaseId, cgp_pattern, post_cgp_pattern, refGenome).run()
      case "all" =>
        new OccurrencesFamily(studyId, releaseId, biospecimenIdColumn, cgp_pattern, post_cgp_pattern, refGenome).run()
        new Occurrences(studyId, releaseId).run()
        new Variants(studyId, releaseId, schema).run()
        new Consequences(studyId, releaseId, cgp_pattern, post_cgp_pattern, refGenome).run()

    }
  }

}
