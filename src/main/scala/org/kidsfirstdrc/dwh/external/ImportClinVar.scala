package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object ImportClinVar extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import ClinVar").getOrCreate()

  import spark.implicits._

  val input = "s3a://kf-strides-variant-parquet-prd/raw/clinvar/clinvar_20201026.vcf.gz"
  val output = "s3a://kf-strides-variant-parquet-prd/public"

  def info_fields(df: DataFrame, excludes: String*): Seq[Column] = {
    df.columns.collect { case c if c.startsWith("INFO") && !excludes.contains(c) => col(c) as c.replace("INFO_", "").toLowerCase }
  }

  val df = vcf(input)
  df.select(chromosome +:
    start +:
    end +:
    name +:
    reference +:
    alternate +:
    ($"INFO_CLNSIG" as "clin_sig_original") +:
    (split(regexp_replace($"INFO_CLNSIGCONF"(0), """\(.\)""", ""), "%3B") as "clin_sig_conflict") +:
    info_fields(df, "INFO_CLNSIG", "INFO_CLNSIGCONF"): _*
  )
    .withColumn("clin_sig",
      when(
        array_contains($"clin_sig_original", "Conflicting_interpretations_of_pathogenicity"),
        array_union(array_remove($"clin_sig_original", "Conflicting_interpretations_of_pathogenicity"), $"clin_sig_conflict")
      ).otherwise($"clin_sig_original")
    )
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .option("path", s"$output/clinvar")
    .saveAsTable("variant.clinvar")
}
