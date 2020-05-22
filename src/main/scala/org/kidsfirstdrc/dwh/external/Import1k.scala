package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object Import1k extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import 1000 Genomes").getOrCreate()

  import spark.implicits._

  val input = "s3://kf-variant-parquet-prd/raw/1000Genomes/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz"
  val output = "s3a://kf-variant-parquet-prd/public"
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
      afr_af,
      eur_af,
      sas_af,
      amr_af,
      eas_af,
      dp
    )
    .repartition($"chromosome")
    .sortWithinPartitions("start")
    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .option("path", s"$output/1000_genomes")
    .saveAsTable("variant.1000_genomes")
}
