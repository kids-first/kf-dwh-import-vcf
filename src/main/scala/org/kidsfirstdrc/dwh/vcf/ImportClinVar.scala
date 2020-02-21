package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.kidsfirstdrc.dwh.vcf.SparkUtils._
import org.kidsfirstdrc.dwh.vcf.SparkUtils.columns._

object ImportClinVar extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import 1000 Genomes").getOrCreate()

  import spark.implicits._

  val input = "s3://kf-variant-parquet-prd/raw/1000Genomes/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz"
  val output = "s3://kf-variant-parquet-prd"
  vcf(input)
    .select(chromosome,
      $"start",
      $"end",
      name,
      reference,
      alternate,
      $"INFO_CLNVSCO" as "clnvsco",
      split($"INFO_GENEINFO", ":") as "gene_info",
      $"INFO_CLNSIGINCL" as "cln_sigincl",
      $"INFO_CLNVI" as "cln_vi",
      $"INFO_CLNDISDB" as "cln_disdb",
      $"INFO_CLNREVSTAT" as "cln_revstat",
      $"INFO_CLNDN" as "cln_dn",
      $"INFO_ALLELEID" as "allele_id",
      $"INFO_ORIGIN"(0) as "origin",
      $"INFO_CLNSIG"(0) as "clin_sig",
      $"INFO_RS"(0) as "rs",
      $"INFO_DBVARID"(0) as "rs",
      $"INFO_CLNHGVS"(0) as "cln_hgvs",
    )
    .withColumn("gene",$"gene_info"(0))
    .withColumn("gene_id",$"gene_info"(1))
    .repartition($"chromosome")
    .sortWithinPartitions("start")
    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .option("path", s"$output/1000_genomes")
    .saveAsTable("variant.1000_genomes")
}
