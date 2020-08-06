package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
import org.apache.spark.sql.types.IntegerType

object ImportDBSNP extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import DBSNP").getOrCreate()

  import spark.implicits._

  val input = "s3a://kf-variant-parquet-prd/raw/dbsnp/GCF_000001405.38.gz"
  val output = "s3a://kf-variant-parquet-prd/public"
  vcf(input)(spark)
    .where($"contigName" like "NC_%")
    .withColumn("chromosome", regexp_extract($"contigName", "NC_(\\d+).(\\d+)", 1).cast("int"))
    .select(
      when($"chromosome" === 23, "X").when($"chromosome" === 24, "Y").when($"chromosome" === 12920, "M").otherwise($"chromosome".cast("string")) as "chromosome",
      start,
      end,
      name,
      reference,
      alternate,
      $"contigName" as "original_contig_name"
    )
    .repartition($"chromosome")
    .sortWithinPartitions("start")
    .write
    .partitionBy("chromosome")
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .option("path", s"$output/dbsnp")
    .saveAsTable("variant.dbsnp")
}
