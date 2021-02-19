package org.kidsfirstdrc.dwh.external

import io.projectglow.functions.lift_over_coordinates
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ImportCancerHotspots extends App {
  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import Cancer Hotspots").getOrCreate()

  import spark.implicits._

  val input = "s3a://kf-strides-variant-parquet-prd/raw/cancerhotspots/cancerhotspots.v2.maf.gz"
  val chain = "/home/hadoop/b37ToHg38.over.chain"
  val output = "s3a://kf-strides-variant-parquet-prd/public"
  private val inputDF: DataFrame = spark.read
    .option("comment", "#")
    .option("header", "true")
    .option("sep", "\t").csv(input)

  val lifted: Dataset[Row] = inputDF
    .withColumn("end", $"End_Position" + 1)
    .drop($"End_Position")
    .withColumn("lifted", lift_over_coordinates($"Chromosome", $"Start_Position", $"end", chain, 0.90))
    .drop("Chromosome", "Start_Position", "end")
    .where($"lifted".isNotNull)
  lifted
    .select(ltrim(col("lifted.contigName"), "chr").as("chromosome") :: $"lifted.start":: $"lifted.end" :: lifted.columns.collect { case x if x != "AF" && !x.contains("_AF") && !x.startsWith("ExAC_") && !x.startsWith("gnomAD_") => col(x).as(x.toLowerCase) }.toList: _*)
    .drop("lifted")
    .write
    .mode("overwrite")
    .format("parquet")
    .option("path", s"$output/cancer_hotspots")
    .saveAsTable("variant.cancer_hotspots_090")

}
