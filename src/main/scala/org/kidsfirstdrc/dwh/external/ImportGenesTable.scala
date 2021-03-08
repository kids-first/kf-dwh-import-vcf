package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.conf.DataSource
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.DataSourceEtl

class ImportGenesTable(runEnv: Environment) extends DataSourceEtl(runEnv) {

  override val destination: DataSource = Public.genes

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(
      Public.omim_gene_set     -> spark.table(s"${Public.omim_gene_set.database}.${Public.omim_gene_set.name}"),
      Public.orphanet_gene_set -> spark.table(s"${Public.orphanet_gene_set.database}.${Public.orphanet_gene_set.name}"),
      Public.hpo_gene_set      -> spark.table(s"${Public.hpo_gene_set.database}.${Public.hpo_gene_set.name}"),
      Public.human_genes       -> spark.table(s"${Public.human_genes.database}.${Public.human_genes.name}")
    )
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
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
      .select($"gene_symbol", $"disorder_id", $"name" as "panel", $"type_of_inheritance" as "inheritance")

    val omim = data(Public.omim_gene_set).select($"omim_gene_id", $"phenotype")

    val hpo = data(Public.hpo_gene_set)
      .select($"entrez_gene_id", $"hpo_term_id", $"hpo_term_name")
      .distinct()
      .withColumn("hpo_term_label", concat($"hpo_term_name", lit(" ("), $"hpo_term_id", lit(")")))

    val withOrphanet = humanGenes
      .join(orphanet, humanGenes("symbol") === orphanet("gene_symbol"), "left")
      .groupBy(humanGenes("symbol"))
      .agg(
        first(struct(humanGenes("*"))) as "hg",
        when(first(orphanet("gene_symbol")).isNotNull,
          collect_list(struct($"disorder_id", $"panel", $"inheritance"))).otherwise(lit(null)) as "orphanet",
      )
      .select($"hg.*", $"orphanet")

    val withHpo = withOrphanet
      .join(hpo, Seq("entrez_gene_id"), "left")
      .groupBy(withOrphanet("symbol"))
      .agg(
        first(struct(withOrphanet("*"))) as "hg",
        when(first(col("entrez_gene_id")).isNotNull, collect_list(struct($"hpo_term_id", $"hpo_term_name", $"hpo_term_label"))).otherwise(lit(null)) as "hpo"
      )
      .select($"hg.*", $"hpo")

    withHpo
      .join(omim, Seq("omim_gene_id"), "left")
      .groupBy(withHpo("symbol"))
      .agg(
        first(struct(withHpo("*"))) as "hg",
        when(first(col("omim_gene_id")).isNotNull, collect_list($"phenotype")).otherwise(lit(null)) as "omim"
      )
      .select($"hg.*", $"omim")
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data.repartition(1))
  }
}

