package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.conf.Ds
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.DsETL

class ImportGenesTable(runEnv: Environment) extends DsETL(runEnv) {

  override val destination: Ds = Public.genes

  override def extract()(implicit spark: SparkSession): Map[Ds, DataFrame] = {
    Map(
      Public.omim_gene_set     -> spark.table(s"${Public.omim_gene_set.database}.${Public.omim_gene_set.name}"),
      Public.orphanet_gene_set -> spark.table(s"${Public.orphanet_gene_set.database}.${Public.orphanet_gene_set.name}"),
      Public.hpo_gene_set      -> spark.table(s"${Public.hpo_gene_set.database}.${Public.hpo_gene_set.name}"),
      Public.human_genes       -> spark.table(s"${Public.human_genes.database}.${Public.human_genes.name}"),
      Public.ddd_gene_set      -> spark.table(s"${Public.ddd_gene_set.database}.${Public.ddd_gene_set.name}"),
      Public.cosmic_gene_set   -> spark.table(s"${Public.cosmic_gene_set.database}.${Public.cosmic_gene_set.name}")
    )
  }

  override def transform(data: Map[Ds, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val humanGenes = data(Public.human_genes)
      .select($"chromosome", $"symbol", $"entrez_gene_id", $"omim_gene_id",
        $"external_references.hgnc" as "hgnc",
        $"ensembl_gene_id",
        $"map_location" as "location",
        $"description" as "name",
        $"synonyms" as "alias",
        regexp_replace($"type_of_gene", "-", "_") as "biotype")

    val orphanet = data(Public.orphanet_gene_set)
      .select($"gene_symbol" as "symbol", $"disorder_id", $"name" as "panel", $"type_of_inheritance" as "inheritance")

    val omim = data(Public.omim_gene_set)
      .select($"omim_gene_id",
        $"phenotype.name" as "name",
        $"phenotype.omim_id" as "omim_id",
        $"phenotype.inheritance" as "inheritance")

    val hpo = data(Public.hpo_gene_set)
      .select($"entrez_gene_id", $"hpo_term_id", $"hpo_term_name")
      .distinct()
      .withColumn("hpo_term_label", concat($"hpo_term_name", lit(" ("), $"hpo_term_id", lit(")")))

    val ddd_gene_set = data(Public.ddd_gene_set)
      .select("disease_name", "symbol")

    val cosmic_gene_set = data(Public.cosmic_gene_set)
      .select("symbol", "tumour_types_germline")

    humanGenes
      .joinAndMergeWith(orphanet, Seq("symbol"), "orphanet")
      .joinAndMergeWith(hpo, Seq("entrez_gene_id"), "hpo")
      .joinAndMergeWith(omim, Seq("omim_gene_id"), "omim")
      .joinAndMergeWith(ddd_gene_set, Seq("symbol"), "ddd")
      .joinAndMergeWith(cosmic_gene_set, Seq("symbol"), "cosmic")

  }

  implicit class DataFrameOps(df: DataFrame) {
    def joinAndMergeWith(gene_set: DataFrame, joinOn: Seq[String], asColumnName: String): DataFrame = {
      df
        .join(gene_set, joinOn, "left")
        .groupBy("symbol")
        .agg(
          first(struct(df("*"))) as "hg",
          collect_list(struct(gene_set.drop(joinOn:_*)("*"))) as asColumnName,
         )
        .select(col("hg.*"), col(asColumnName))
        //TODO find better solution than to_json() === lit("[{}]")
        .withColumn(asColumnName, when(to_json(col(asColumnName)) === lit("[{}]"), array()).otherwise(col(asColumnName)))
    }
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data.repartition(1))
  }
}

