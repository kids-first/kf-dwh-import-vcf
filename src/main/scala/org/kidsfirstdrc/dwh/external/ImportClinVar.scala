package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object ImportClinVar extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import 1000 Genomes").getOrCreate()

  import spark.implicits._
  
  val input = "s3://kf-variant-parquet-prd/raw/clinvar/clinvar_20200217.vcf.gz"
  val output = "s3://kf-variant-parquet-prd/public"
  vcf(input)
    .select(chromosome,
      start,
      end,
      name,
      reference,
      alternate,
      $"INFO_CLNSIG" as "clin_sig_original",
      split(regexp_replace($"INFO_CLNSIGCONF"(0), """\(.\)""", ""), "%3B") as "clin_sig_conflict"
    )
    .withColumn("clin_sig",
      when(
        array_contains($"clin_sig_original", "Conflicting_interpretations_of_pathogenicity"),
        array_union(array_remove($"clin_sig_original", "Conflicting_interpretations_of_pathogenicity"), $"clin_sig_original")
      ).otherwise($"clin_sig_original")
    )
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .option("path", s"$output/clinvar")
    .saveAsTable("variant.clinvar")
}
