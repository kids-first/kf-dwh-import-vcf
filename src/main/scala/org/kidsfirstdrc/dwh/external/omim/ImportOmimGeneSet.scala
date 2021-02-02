package org.kidsfirstdrc.dwh.external.omim

import org.apache.spark.sql.functions.{col, explode, split}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.external.omim.OmimPhenotype.parse_pheno
import org.kidsfirstdrc.dwh.utils.EtlJob

object ImportOmimGeneSet extends App with EtlJob {

  override val database = "variant"
  override val tableName = "omim_gene_set"

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import OMIM Geneset").getOrCreate()

  override def extract(input: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("csv")
      .option("inferSchema", "true")
      .option("comment", "#")
      .option("header", "false")
      .option("sep", "\t")
      .load(input)
  }

  override def transform(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      .select(
        col("_c0") as "chromosome",
        col("_c1") as "start",
        col("_c2") as "end",
        col("_c3") as "cypto_location",
        col("_c4") as "computed_cypto_location",
        col("_c5") as "omim_gene_id",
        split(col("_c6"), ", ") as "symbols",
        col("_c7") as "name",
        col("_c8") as "approved_symbol",
        col("_c9") as "entrez_gene_id",
        col("_c10") as "ensembl_gene_id",
        col("_c11") as "documentation",
        split(col("_c12"), ";") as "phenotypes")
      .withColumn("raw_phenotype", explode(col("phenotypes")))
      .drop("phenotypes")
      .withColumn("phenotype", parse_pheno(col("raw_phenotype")))
      .drop("raw_phenotype")
  }

  override def load(data: DataFrame, output: String)(implicit spark: SparkSession): Unit = {
    data.coalesce(1)
      .write
      .mode("overwrite")
      .format("parquet")
      .option("path", s"$output/omim_gene_set")
      .saveAsTable(s"$database.$tableName")
    spark.sql(s"create or replace view variant_live.$tableName as select * from variant.$tableName")
  }

  val input = "s3a://kf-strides-variant-parquet-prd/raw/omim/genemap2.txt"
  val output = "s3a://kf-strides-variant-parquet-prd/public"

  val inputDf = extract(input)
  val outputDf = transform(inputDf)
  load(outputDf, output)
}

