package org.kidsfirstdrc.dwh.external.omim

import org.apache.spark.sql.functions.{col, explode, split}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.conf.DataSource
import org.kidsfirstdrc.dwh.conf.Environment._
import org.kidsfirstdrc.dwh.external.omim.OmimPhenotype.parse_pheno
import org.kidsfirstdrc.dwh.jobs.DataSourceEtl

class ImportOmimGeneSet(runEnv: Environment) extends DataSourceEtl(runEnv) {

  override val destination: DataSource = Public.omim_gene_set

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("comment", "#")
      .option("header", "false")
      .option("sep", "\t")
      .load(Raw.omim_genemap2.path)

    Map(Raw.omim_genemap2 -> df)
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    data(Raw.omim_genemap2)
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

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = super.load(data.coalesce(1))
}

