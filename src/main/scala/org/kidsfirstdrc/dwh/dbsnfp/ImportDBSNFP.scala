package org.kidsfirstdrc.dwh.dbsnfp

import org.apache.spark.sql.{SaveMode, SparkSession}

object ImportDBSNFP extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("VCF Import to DWH").getOrCreate()

  import spark.implicits._

  spark.read
    .option("sep", "\t")
    .option("header", "true")
    .option("nullValue", ".")
    .schema(schema.schema)
    .csv("s3a://kf-variant-parquet-prd/dbSNFP/variant/*.gz")
    .withColumn("start", $"position_1-based" - 1)
    .write.mode(SaveMode.Overwrite)
    .partitionBy("chromosome")
    .format("parquet")
    .option("path", "s3a://kf-variant-parquet-prd/dbSNFP/parquet/variant")
    .saveAsTable("variant.dbsnfp")


}
