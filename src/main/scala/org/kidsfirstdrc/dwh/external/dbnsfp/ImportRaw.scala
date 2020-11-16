package org.kidsfirstdrc.dwh.external.dbnsfp

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
object ImportRaw extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import DBNSFP Raw").getOrCreate()

  spark.read
    .option("sep", "\t")
    .option("header", "true")
    .option("nullValue", ".")
    .schema(schema.schema)
    .csv("s3a://kf-strides-variant-parquet-prd/raw/dbNSFP/*.gz")
    .withColumnRenamed("position_1-based", "start")
    .write.mode(SaveMode.Overwrite)
    .partitionBy("chromosome")
    .format("parquet")
    .option("path", "s3a://kf-strides-variant-parquet-prd/public/dbnsfp/variant")
    .saveAsTable("variant.dbnsfp")


}

