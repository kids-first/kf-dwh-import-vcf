package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.commons.config.Configuration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog._
import org.kidsfirstdrc.dwh.jobs.StandardETL

import java.time.LocalDateTime

class ImportDDDGeneCensus()(implicit conf: Configuration)
    extends StandardETL(Public.ddd_gene_set)(conf) {

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      Raw.ddd_gene_census.id -> spark.read
        .option("header", "true")
        .csv(Raw.ddd_gene_census.location)
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(Raw.ddd_gene_census.id)
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

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(data.coalesce(1))
  }
}
