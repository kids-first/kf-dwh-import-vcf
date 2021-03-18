package org.kidsfirstdrc.dwh.es.json

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import org.apache.spark.sql.SparkSession

object PrepareIndex extends App {

  val Array(jobType, releaseId) = args

  implicit lazy val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Export $jobType - $releaseId").getOrCreate()

  implicit val conf: Configuration = Configuration(List(
    StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd")
  ))

  jobType match {
    case "gene_centric" => new GeneCentricIndexJson().run()
    case "genomic_suggestions" => new GenomicSuggestionsIndexJson(releaseId).run()
    case "variant_centric" => new VariantCentricIndexJson(releaseId).run()
  }

}