package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.SparkSession

object ImportHPOGeneSet extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import HPO Geneset").getOrCreate()

  val input = "s3a://kf-variant-parquet-prd/raw/hpo/genes_to_phenotype.txt"
  val output = "s3://kf-variant-parquet-prd"
  spark.read.format("csv")
    .option("inferSchema", "true")
    .option("comment", "#")
    .option("header", "false")
    .option("sep", "\t")
    .option("nullValue", "-").load(input)
    .withColumnRenamed("_c0", "entrez_gene_id")
    .withColumnRenamed("_c1", "symbol")
    .withColumnRenamed("_c2", "hpo_term_id")
    .withColumnRenamed("_c3", "hpo_term_name")
    .withColumnRenamed("_c4", "frequency_raw")
    .withColumnRenamed("_c5", "frequency_hpo")
    .withColumnRenamed("_c6", "source_info")
    .withColumnRenamed("_c7", "source")
    .withColumnRenamed("_c8", "source_id")
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .option("path", s"$output/hpo_gene_set")
    .saveAsTable("variant.hpo_gene_set")
}
