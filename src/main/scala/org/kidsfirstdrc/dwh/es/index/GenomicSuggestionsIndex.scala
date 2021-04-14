package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.{DataSource, ETL}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Es, Public}
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locus

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
      .selectLocus(col("symbol"), col("aa_change"), col("ensembl_gene_id"), col("ensembl_transcript_id"))
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
      .withColumn("ensembl_gene_id", when(col("ensembl_gene_id").isNull, lit("")).otherwise(trim(col("ensembl_gene_id"))))
      .withColumn("ensembl_transcript_id", when(col("ensembl_transcript_id").isNull, lit("")).otherwise(trim(col("ensembl_transcript_id"))))
      .withColumn("aa_change", when(col("aa_change").isNull, lit("")).otherwise(trim(col("aa_change"))))
      .withColumn("symbol", when(col("symbol").isNull, lit("")).otherwise(trim(col("symbol"))))
      .withColumn("symbol_aa_change", trim(concat_ws(" ", col("symbol"), col("aa_change"))))
      .groupBy(locus:_*)
      .agg(
        collect_set(col("symbol")) as "symbol",
        collect_set(col("aa_change")) as "aa_change",
        collect_set(col("symbol_aa_change")) as "symbol_aa_change",
        collect_set(col("ensembl_gene_id")) as "ensembl_gene_id",
        collect_set(col("ensembl_transcript_id")) as "ensembl_transcript_id"
      )

    groupedByLocusConsequences.show(false)

    variants
      .withColumn("clinvar_id", when(col("clinvar_id").isNull, lit("")).otherwise(trim(col("clinvar_id"))))
      .withColumn("hgvsg", when(col("hgvsg").isNull, lit("")).otherwise(trim(col("hgvsg"))))
      .withColumn("rsnumber", when(col("name").isNull, lit("")).otherwise(trim(col("name"))))
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
            col("ensembl_transcript_id")
          )), "") as "input",
          lit(variantSymbolWeight) as "weight"),
        ))
      .withColumn("symbol", col("symbol")(0))
      .select(indexColumns.head, indexColumns.tail:_*)
  }

  def getGenesSuggest(genes: DataFrame): DataFrame = {
    genes
      .withColumn("ensembl_gene_id", when(col("ensembl_gene_id").isNull, lit("")).otherwise(trim(col("ensembl_gene_id"))))
      .withColumn("symbol", when(col("symbol").isNull, lit("")).otherwise(trim(col("symbol"))))
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
