package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.SparkSession

object AnnotationToVCF extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("VCF Import to DWH").getOrCreate()
  import spark.implicits._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SaveMode
  spark.table("annotations_sd_9pyzahhe_re_123456")
    .select($"chromosome" as "contigName", $"start" as "start", $"end" as "end", array(when($"name".isNull,lit(".")).otherwise($"name")) as "names", $"reference" as "referenceAllele", array($"alternate") as "alternateAlleles", lit(".") as "qual", array(lit("PASS")) as "filters", lit(".") as "INFO")
    .repartition($"contigName")
    .sortWithinPartitions("start")
    .write
    .format("bigvcf")
    .mode(SaveMode.Overwrite)
    .save("s3a://kf-variant-parquet-prd/test_write_vcf/sd_9pyzahhe.vcf.bgz")
}
