package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{locus, locusColumNames}
import bio.ferlab.datalake.spark3.implicits.SparkUtils.getColumnOrElse
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Es}
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._

import java.time.LocalDateTime
import scala.util.{Success, Try}

class VariantsSuggestionsIndex(schema: String, releaseId: String)(override implicit val conf: Configuration)
    extends ETL() {

  val destination: DatasetConf = Es.variants_suggestions

  final val variantSymbolAaChangeWeight = 4
  final val variantSymbolWeight         = 2

  final val indexColumns =
    List("type", "locus", "suggestion_id", "hgvsg", "suggest", "chromosome", "rsnumber", "symbol_aa_change")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    import spark.implicits._
    val occurrences: DataFrame = spark.read
      .parquet(s"${Clinical.variants.rootPath}/variants/variants_$releaseId")
      .withColumn("study", explode(col("studies")))
      .select("study")
      .distinct
      .as[String]
      .collect()
      .map(studyId =>
        Try(
          spark.table(s"$schema.occurrences_${studyId.toLowerCase}")
        )
      )
      .collect { case Success(df) => df }
      .reduce(_ unionByName _)

    Map(
      Clinical.occurrences.id -> occurrences,
      Clinical.variants.id -> spark.read.parquet(
        s"${Clinical.variants.rootPath}/variants/variants_$releaseId"
      ),
      Clinical.consequences.id -> spark.read.parquet(
        s"${Clinical.consequences.rootPath}/consequences/consequences_$releaseId"
      )
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val distinctOccurrences = data(Clinical.occurrences.id)
      .selectLocus(col("has_alt"))
      .where(col("has_alt") === 1)
      .drop("has_alt")
      .dropDuplicates(locusColumNames.head, locusColumNames.tail:_*)

    val variants =
      data(Clinical.variants.id)
        .selectLocus(
          col("hgvsg"),
          col("name"),
          col("clinvar_id"))
        .join(distinctOccurrences, locusColumNames, "inner")

    val consequences = data(Clinical.consequences.id)
      .selectLocus(
        col("symbol"),
        col("aa_change"),
        col("ensembl_gene_id"),
        col("ensembl_transcript_id"),
        col("refseq_mrna_id"),
        col("refseq_protein_id")
      )
      .dropDuplicates()

    getVariantSuggest(variants, consequences)
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    data.write
      .mode(SaveMode.Overwrite)
      .option("format", destination.format.sparkFormat)
      .option("path", s"${destination.location}_$releaseId")
      .saveAsTable(s"${destination.table.get.fullName}_${releaseId}")
    data
  }

  def getVariantSuggest(variants: DataFrame, consequence: DataFrame): DataFrame = {
    val groupedByLocusConsequences = consequence
      .withColumn("symbol_aa_change", concat_ws(" ", col("symbol"), col("aa_change")))
      .withColumn("ensembl_gene_id", getColumnOrElse("ensembl_gene_id"))
      .withColumn("ensembl_transcript_id", getColumnOrElse("ensembl_transcript_id"))
      .withColumn("refseq_mrna_id", getColumnOrElse("refseq_mrna_id"))
      .withColumn("refseq_protein_id", getColumnOrElse("refseq_protein_id"))
      .groupBy(locus: _*)
      .agg(
        array_remove(collect_set(col("aa_change")), "") as "aa_change",
        array_remove(collect_set(col("symbol_aa_change")), "") as "symbol_aa_change",
        collect_set(col("ensembl_gene_id")) as "ensembl_gene_id",
        collect_set(col("ensembl_transcript_id")) as "ensembl_transcript_id",
        collect_set(col("refseq_mrna_id")) as "refseq_mrna_id",
        collect_set(col("refseq_protein_id")) as "refseq_protein_id"
      )

    variants
      .withColumn("clinvar_id", getColumnOrElse("clinvar_id"))
      .withColumn("hgvsg", getColumnOrElse("hgvsg"))
      .withColumn("rsnumber", getColumnOrElse("name"))
      .joinByLocus(groupedByLocusConsequences, "left")
      .withColumn("type", lit("variant"))
      .withColumn("locus", concat_ws("-", locus: _*))
      .withColumn(
        "suggestion_id",
        sha1(col("locus"))
      ) //this maps to `hash` column in variant_centric index
      .withColumn("hgvsg", col("hgvsg"))
      .withColumn(
        "suggest",
        array(
          struct(
            array_remove(
              flatten(
                array(
                  array(col("hgvsg")),
                  array(col("locus")),
                  array(col("rsnumber")),
                  array(col("clinvar_id"))
                )
              ),
              ""
            ) as "input",
            lit(variantSymbolAaChangeWeight) as "weight"
          ),
          struct(
            array_distinct(
              array_remove(
                flatten(
                  array(
                    col("aa_change"),
                    col("symbol_aa_change"),
                    col("ensembl_gene_id"),
                    col("ensembl_transcript_id"),
                    col("refseq_mrna_id"),
                    col("refseq_protein_id")
                  )
                ),
                ""
              )
            ) as "input",
            lit(variantSymbolWeight) as "weight"
          )
        )
      )
      .select(indexColumns.head, indexColumns.tail: _*)
  }
}
