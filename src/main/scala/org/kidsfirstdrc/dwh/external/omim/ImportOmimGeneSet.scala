package org.kidsfirstdrc.dwh.external.omim

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{broadcast, explode, split}
import org.kidsfirstdrc.dwh.external.omim.OmimPhenotype.parse_pheno

object ImportOmimGeneSet extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import OMIM Geneset").getOrCreate()

  import spark.implicits._

  val input = "s3a://kf-variant-parquet-prd/raw/omim/genemap2.txt"
  val output = "s3a://kf-variant-parquet-prd/public"


  spark.read.format("csv")
    .option("inferSchema", "true")
    .option("comment", "#")
    .option("header", "false")
    .option("sep", "\t")
    .load(input)
    .select(
      $"_c0" as "chromosome",
      $"_c1" as "start",
      $"_c2" as "end",
      $"_c3" as "cypto_location",
      $"_c4" as "computed_cypto_location",
      $"_c5" as "omim_gene_id",
      split($"_c6", ", ") as "symbols",
      $"_c7" as "name",
      $"_c8" as "approved_symbol",
      $"_c9" as "entrez_gene_id",
      $"_c10" as "ensembl_gene_id",
      $"_c11" as "comments",
      split($"_c12", ";") as "phenotypes")
    .withColumn("raw_phenotype", explode($"phenotypes"))
    .drop("phenotypes")
    .withColumn("phenotype", parse_pheno($"raw_phenotype"))
    .drop("raw_phenotype")
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .option("path", s"$output/omim_gene_set")
    .saveAsTable("variant.omim_gene_set")

  spark.sql("create or replace view variant_live.omim_gene_set as select * from variant.omim_gene_set")

}


