package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.DataSource
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.StandardETL

class ImportHPOGeneSet(runEnv: Environment)(implicit conf: Configuration)
  extends StandardETL(Public.hpo_gene_set)(runEnv, conf) {

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    val inputDF: DataFrame = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("comment", "#")
      .option("header", "false")
      .option("sep", "\t")
      .option("nullValue", "-")
      .load(Raw.hpo_genes_to_phenotype.location)

    val human_genes = broadcast(spark.table("variant.human_genes")
      .select("entrez_gene_id", "ensembl_gene_id"))

    Map(
      Raw.hpo_genes_to_phenotype -> inputDF,
      Public.human_genes ->  human_genes
    )
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val human_genes = data(Public.human_genes)
    val inputDF =
      data(Raw.hpo_genes_to_phenotype)
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
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data.coalesce(1))
  }
}
