package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.commons.config.{Configuration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.conf.Catalog

object PrepareIndex extends App {

  val Array(jobType, releaseId) = args

  implicit lazy val spark: SparkSession = SparkSession.builder
    .config(
      "hive.metastore.client.factory.class",
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )
    .enableHiveSupport()
    .appName(s"Export $jobType - $releaseId")
    .getOrCreate()

  implicit val conf: Configuration = Configuration(
    List(StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd/portal", S3)),
    sources = Catalog.sources.toList
  )

  jobType match {
    case "gene_centric"         => new GeneCentricIndex(releaseId).run()
    case "genes_suggestions"    => new GenesSuggestionsIndex(releaseId).run()
    case "variants_suggestions" => new VariantsSuggestionsIndex("portal", releaseId).run()
    case "variant_centric"      => new VariantCentricIndex("portal", releaseId).run()
  }

}
