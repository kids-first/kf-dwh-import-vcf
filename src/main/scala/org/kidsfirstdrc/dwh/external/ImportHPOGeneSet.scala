package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast
object ImportHPOGeneSet extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import HPO Geneset").getOrCreate()

  val input = "s3a://kf-variant-parquet-prd/raw/hpo/genes_to_phenotype.txt"
  val output = "s3a://kf-variant-parquet-prd/public"

  val human_genes = broadcast(spark.table("variant.human_genes").select("entrez_gene_id", "ensembl_gene_id"))

  val inputDF: DataFrame = spark.read.format("csv")
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
  inputDF
    .join(spark.table("variant.human_genes"), inputDF("entrez_gene_id") === human_genes("entrez_gene_id"))
    .select(inputDF("*"), human_genes("ensembl_gene_id"))
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .option("path", s"$output/hpo_gene_set")
    .saveAsTable("variant.hpo_gene_set")
}
