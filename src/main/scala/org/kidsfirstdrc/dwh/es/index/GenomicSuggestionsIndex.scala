package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.spark3.config.Configuration
import bio.ferlab.datalake.spark3.etl.{DataSource, ETL}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Es, Public}
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locus
import org.kidsfirstdrc.dwh.utils.SparkUtils.getColumnOrElse

class GenomicSuggestionsIndex(releaseId: String)
                             (override implicit val conf: Configuration) extends ETL(Es.genomic_suggestions) {

  final val geneSymbolWeight = 5
  final val geneAliasesWeight = 3
  final val variantSymbolAaChangeWeight = 4
  final val variantSymbolWeight = 2

  final val indexColumns = List("type", "symbol", "locus", "suggestion_id", "hgvsg", "suggest", "chromosome", "rsnumber")

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(
      Public.genes -> spark.table(s"${Public.genes.database}.${Public.genes.name}"),
      Clinical.variants -> spark.read.parquet(s"${Clinical.variants.rootPath}/variants/variants_$releaseId"),
      Clinical.consequences -> spark.read.parquet(s"${Clinical.consequences.rootPath}/consequences/consequences_$releaseId")
    )
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val genes = data(Public.genes).select("symbol", "alias", "ensembl_gene_id")
    val variants = data(Clinical.variants).selectLocus(col("hgvsg"), col("name"), col("clinvar_id"))
    val consequences = data(Clinical.consequences)
      .selectLocus(col("symbol"), col("aa_change"), col("ensembl_gene_id"),
        col("ensembl_transcript_id"), col("refseq_mrna_id"), col("refseq_protein_id"))
      .dropDuplicates()

    getGenesSuggest(genes).unionByName(getVariantSuggest(variants, consequences))
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .option("format", "parquet")
      .option("path", s"${destination.location}_$releaseId")
      .saveAsTable(s"${destination.database}.${destination.name}_${releaseId}")
    data
  }

  def getVariantSuggest(variants: DataFrame, consequence: DataFrame): DataFrame = {

    val groupedByLocusConsequences = consequence
      .withColumn("ensembl_gene_id", getColumnOrElse("ensembl_gene_id"))
      .withColumn("ensembl_transcript_id", getColumnOrElse("ensembl_transcript_id"))
      .withColumn("aa_change", getColumnOrElse("aa_change"))
      .withColumn("symbol", getColumnOrElse("symbol"))
      .withColumn("symbol_aa_change", trim(concat_ws(" ", col("symbol"), col("aa_change"))))
      .withColumn("refseq_mrna_id", getColumnOrElse("refseq_mrna_id"))
      .withColumn("refseq_protein_id", getColumnOrElse("refseq_protein_id"))
      .groupBy(locus:_*)
      .agg(
        collect_set(col("symbol")) as "symbol",
        collect_set(col("aa_change")) as "aa_change",
        collect_set(col("symbol_aa_change")) as "symbol_aa_change",
        collect_set(col("ensembl_gene_id")) as "ensembl_gene_id",
        collect_set(col("ensembl_transcript_id")) as "ensembl_transcript_id",
        collect_set(col("refseq_mrna_id")) as "refseq_mrna_id",
        collect_set(col("refseq_protein_id")) as "refseq_protein_id",
      )

    variants
      .withColumn("clinvar_id", getColumnOrElse("clinvar_id"))
      .withColumn("hgvsg", getColumnOrElse("hgvsg"))
      .withColumn("rsnumber", getColumnOrElse("name"))
      .joinByLocus(groupedByLocusConsequences, "left")
      .withColumn("type", lit("variant"))
      .withColumn("locus", concat_ws("-", locus:_*))
      .withColumn("suggestion_id", sha1(col("locus"))) //this maps to `hash` column in variant_centric index
      .withColumn("hgvsg", col("hgvsg"))
      .withColumn("suggest", array(
        struct(
          array_remove(flatten(array(
            col("symbol_aa_change"),
            array(col("hgvsg")),
            array(col("locus")),
            array(col("rsnumber")),
            array(col("clinvar_id")))), "") as "input",
          lit(variantSymbolAaChangeWeight) as "weight"),
        struct(
          array_remove(flatten(array(
            col("symbol"),
            col("ensembl_gene_id"),
            col("ensembl_transcript_id"),
            col("refseq_mrna_id"),
            col("refseq_protein_id")
          )), "") as "input",
          lit(variantSymbolWeight) as "weight"),
        ))
      .withColumn("symbol", col("symbol")(0))
      .select(indexColumns.head, indexColumns.tail:_*)
  }

  def getGenesSuggest(genes: DataFrame): DataFrame = {
    genes
      .withColumn("ensembl_gene_id", getColumnOrElse("ensembl_gene_id"))
      .withColumn("symbol", getColumnOrElse("symbol"))
      .withColumn("type", lit("gene"))
      .withColumn("suggestion_id", sha1(col("symbol"))) //this maps to `hash` column in gene_centric index
      .withColumn("hgvsg", lit(null).cast(StringType))
      .withColumn("rsnumber", lit(null).cast(StringType))
      .withColumn("locus", lit(null).cast(StringType))
      .withColumn("chromosome", lit(null).cast(StringType))
      .withColumn("suggest", array(
        struct(
          array(col("symbol")) as "input",
          lit(geneSymbolWeight) as "weight"
        ), struct(
          array_remove(flatten(array(
            functions.transform(col("alias"), c => when(c.isNull, lit("")).otherwise(c)),
            array(col("ensembl_gene_id"))
          )), "") as "input",
          lit(geneAliasesWeight) as "weight"
      )))
      .select(indexColumns.head, indexColumns.tail:_*)
  }
}
