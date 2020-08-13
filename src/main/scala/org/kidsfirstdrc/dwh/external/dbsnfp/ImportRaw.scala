package org.kidsfirstdrc.dwh.external.dbsnfp

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object ImportRaw extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import DBSNFP Raw").getOrCreate()

  spark.read
    .option("sep", "\t")
    .option("header", "true")
    .option("nullValue", ".")
    .schema(schema.schema)
    .csv("s3a://kf-variant-parquet-prd/dbSNFP/variant/*.gz")
    .withColumnRenamed("position_1-based", "start")
    .write.mode(SaveMode.Overwrite)
    .partitionBy("chromosome")
    .format("parquet")
    .option("path", "s3a://kf-variant-parquet-prd/public/dbnsfp/parquet/variant")
    .saveAsTable("variant.dbnsfp")


}

