package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.conf.DataSource
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.DataSourceEtl

class ImportDDDGeneCensus(runEnv: Environment) extends DataSourceEtl(runEnv) {

  override val destination: DataSource = Public.ddd_gene_set

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(Raw.ddd_gene_census -> spark.read.option("header", "true").csv(Raw.ddd_gene_census.path))
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(Raw.ddd_gene_census)
      .select(
        $"gene symbol" as "symbol",
        $"gene mim" as "omim_gene_id",
        $"disease name" as "disease_name",
        $"disease mim" as "disease_omim_id",
        $"DDD category" as "ddd_category",
        $"mutation consequence" as "mutation_consequence",
        split($"phenotypes", ";") as "phenotypes",
        split($"organ specificity list", ";") as "organ_specificity",
        $"panel",
        $"hgnc id" as "hgnc_id"
      )
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data.coalesce(1))
  }
}
