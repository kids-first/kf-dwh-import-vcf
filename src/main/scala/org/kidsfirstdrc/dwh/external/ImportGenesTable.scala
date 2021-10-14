package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.commons.config.Configuration
import bio.ferlab.datalake.spark3.implicits.SparkUtils.removeEmptyObjectsIn
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.jobs.StandardETL

import java.time.LocalDateTime

class ImportGenesTable()(implicit conf: Configuration) extends StandardETL(Public.genes)(conf) {

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      Public.omim_gene_set.id     -> spark.table(s"${Public.omim_gene_set.table.get.fullName}"),
      Public.orphanet_gene_set.id -> spark.table(s"${Public.orphanet_gene_set.table.get.fullName}"),
      Public.hpo_gene_set.id      -> spark.table(s"${Public.hpo_gene_set.table.get.fullName}"),
      Public.human_genes.id       -> spark.table(s"${Public.human_genes.table.get.fullName}"),
      Public.ddd_gene_set.id      -> spark.table(s"${Public.ddd_gene_set.table.get.fullName}"),
      Public.cosmic_gene_set.id   -> spark.table(s"${Public.cosmic_gene_set.table.get.fullName}")
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val humanGenes = data(Public.human_genes.id)
      .select(
        $"chromosome",
        $"symbol",
        $"entrez_gene_id",
        $"omim_gene_id",
        $"external_references.hgnc" as "hgnc",
        $"ensembl_gene_id",
        $"map_location" as "location",
        $"description" as "name",
        $"synonyms" as "alias",
        regexp_replace($"type_of_gene", "-", "_") as "biotype"
      )

    val orphanet = data(Public.orphanet_gene_set.id)
      .select(
        $"gene_symbol" as "symbol",
        $"disorder_id",
        $"name" as "panel",
        $"type_of_inheritance" as "inheritance"
      )

    val omim = data(Public.omim_gene_set.id)
      .where($"phenotype.name".isNotNull)
      .select(
        $"omim_gene_id",
        $"phenotype.name" as "name",
        $"phenotype.omim_id" as "omim_id",
        $"phenotype.inheritance" as "inheritance",
        $"phenotype.inheritance_code" as "inheritance_code"
      )

    val hpo = data(Public.hpo_gene_set.id)
      .select($"entrez_gene_id", $"hpo_term_id", $"hpo_term_name")
      .distinct()
      .withColumn("hpo_term_label", concat($"hpo_term_name", lit(" ("), $"hpo_term_id", lit(")")))

    val ddd_gene_set = data(Public.ddd_gene_set.id)
      .select("disease_name", "symbol")

    val cosmic_gene_set = data(Public.cosmic_gene_set.id)
      .select("symbol", "tumour_types_germline")

    humanGenes
      .joinAndMergeWith(orphanet, Seq("symbol"), "orphanet")
      .joinAndMergeWith(hpo, Seq("entrez_gene_id"), "hpo")
      .joinAndMergeWith(omim, Seq("omim_gene_id"), "omim")
      .joinAndMergeWith(ddd_gene_set, Seq("symbol"), "ddd")
      .joinAndMergeWith(cosmic_gene_set, Seq("symbol"), "cosmic")

  }

  implicit class DataFrameOps(df: DataFrame) {
    def joinAndMergeWith(
        gene_set: DataFrame,
        joinOn: Seq[String],
        asColumnName: String
    ): DataFrame = {
      df
        .join(gene_set, joinOn, "left")
        .groupBy("symbol")
        .agg(
          first(struct(df("*"))) as "hg",
          collect_list(struct(gene_set.drop(joinOn: _*)("*"))) as asColumnName
        )
        .select(col("hg.*"), col(asColumnName))
        .withColumn(asColumnName, removeEmptyObjectsIn(asColumnName))
    }
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(data.repartition(1))
  }
}
