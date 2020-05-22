package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object ImportTopMed extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import TopMed - Bravo").getOrCreate()

  import spark.implicits._

  val input = "s3://kf-variant-parquet-prd/raw/topmed/bravo-dbsnp-all.vcf.gz"
  val output = "s3://kf-variant-parquet-prd/public"
  vcf(input)(spark)
    .select(chromosome,
      start,
      end,
      name,
      reference,
      alternate,
      ac,
      af,
      an,
      $"INFO_HOM"(0) as "hom",
      $"INFO_HET"(0) as "het",
      $"qual",
      $"filters",
      when(size($"filters") === 1 && $"filters"(0) === "PASS", "PASS").when(array_contains($"filters","PASS"), "PASS+FAIL").otherwise("FAIL") as "qual_filter"
    )
    .repartition($"chromosome")
    .sortWithinPartitions("start")
    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .option("path", s"$output/topmed_bravo")
    .saveAsTable("variant.topmed_bravo")
}
