package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object ImportDDDGeneCensus extends App {
  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import DDD Gene Census").getOrCreate()

  import spark.implicits._
  val input = "s3a://kf-strides-variant-parquet-prd/raw/ddd/DDG2P_17_5_2020.csv"
  val output = "s3a://kf-strides-variant-parquet-prd/public"
  spark.read.option("header", "true").csv(input)
    .select(
      $"gene symbol" as "symbol",
      $"gene mim" as "omim_gene_id",
      $"disease name" as "disease_name",
      $"disease mim" as "disease_omim_id",
      $"DDD category" as "ddd_category",
      $"mutation consequence" as "mutation_consequence",
      split($"phenotypes", ";") as "phenotypes",
      split($"organ specificity list", ";") as "organ_specificity",
      $"panel",
      $"hgnc id" as "hgnc_id"
    )
    .write
    .mode("overwrite")
    .format("parquet")
    .option("path", s"$output/ddd_gene_set")
    .saveAsTable("variant_live.ddd_gene_set")

}
