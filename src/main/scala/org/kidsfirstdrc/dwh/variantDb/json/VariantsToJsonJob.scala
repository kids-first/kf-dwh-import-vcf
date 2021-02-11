package org.kidsfirstdrc.dwh.variantDb.json

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{explode, _}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.join.JoinConsequences
import org.kidsfirstdrc.dwh.utils.Catalog.Public
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locus
import org.kidsfirstdrc.dwh.utils.{Environment, MultiSourceEtlJob}
import org.kidsfirstdrc.dwh.variantDb.json.VariantsToJsonJob._
import org.kidsfirstdrc.dwh.vcf.Variants

import scala.collection.mutable

class VariantsToJsonJob(releaseId: String) extends MultiSourceEtlJob(Environment.DEV) {

  override val database: String = "variant"
  override val tableName: String = "variant_index"

  override def extract(input: String)(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      Variants.TABLE_NAME -> spark.table(s"variant.${Variants.TABLE_NAME}_$releaseId"),
      JoinConsequences.TABLE_NAME -> spark.table(s"variant.${JoinConsequences.TABLE_NAME}_$releaseId"),
      Public.omim_gene_set.name -> spark.table(s"${Public.omim_gene_set.database}.${Public.omim_gene_set.name}")
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val variants = data(Variants.TABLE_NAME)

    val consequences = data(JoinConsequences.TABLE_NAME)

    val consequencesColumns = Set("chromosome", "start", "reference", "alternate", "symbol", "ensembl_gene_id", "consequences",
      "name", "impact", "symbol", "strand", "biotype", "variant_class", "exon", "intron", "hgvsc", "hgvsp", "hgvsg",
      "cds_position", "cdna_position", "protein_position", "amino_acids", "codons", "canonical", "aa_change", "coding_dna_change",
      "ensembl_transcript_id", "ensembl_regulatory_id", "feature_type", "scores")

    val omim = data(Public.omim_gene_set.name)
      .select("ensembl_gene_id", "entrez_gene_id", "omim_gene_id")

    val consequenceColumnsWithOmim: Set[String] = consequencesColumns ++ omim.columns.toSet -- Set("chromosome", "start", "reference", "alternate")

    val consequencesWithOmim = consequences
      .withScores
      .join(omim, Seq("ensembl_gene_id"), "left")
      .withColumn("consequence", struct(consequenceColumnsWithOmim.toSeq.map(col):_*))
      .groupBy(locus:_*)
      .agg(collect_set(col("consequence")).as("consequences"))

    variants
      .withStudies
      .withFrequencies
      .withClinVar
      .select("chromosome", "start", "end", "reference", "alternate", "studies", "frequencies", "clinvar", "dbsnp_id", "release_id")
      .joinByLocus(consequencesWithOmim)
  }

  override def load(data: DataFrame, output: String)(implicit spark: SparkSession): DataFrame = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .json(s"$output/tmp/${this.tableName}_${this.releaseId}")
    data
  }
}

object VariantsToJsonJob {

  private def frequenciesForGnomad(colName: String): Column = {
    struct(
      col(s"$colName.ac") as "ac",
      col(s"$colName.an") as "an",
      col(s"$colName.af") as "af",
      col(s"$colName.hom") as "homozygotes"
    ).as(colName)
  }

  private def frequenciesForPrefix(prefix: String): Column = {
    struct(
      col(s"${prefix}_ac") as "ac",
      col(s"${prefix}_an") as "an",
      col(s"${prefix}_af") as "af",
      col(s"${prefix}_homozygotes") as "homozygotes",
      col(s"${prefix}_heterozygotes") as "heterozygotes"
    ).as(prefix)
  }

  private def frequenciesByStudiesFor(prefix: String): Column = {
    struct(
      col(s"${prefix}_ac_by_study")(col("study_id")) as "ac",
      col(s"${prefix}_an_by_study")(col("study_id")) as "an",
      col(s"${prefix}_af_by_study")(col("study_id")) as "af",
      col(s"${prefix}_homozygotes_by_study")(col("study_id")) as "homozygotes",
      col(s"${prefix}_heterozygotes_by_study")(col("study_id")) as "heterozygotes"
    ).as(prefix)
  }

  implicit class DataFrameOperations(df: DataFrame) {

    def joinByLocus(df2: DataFrame): DataFrame = {
      df.join(df2, locus.map(_.toString), "left")
    }

    def short_consent_codes_udf: UserDefinedFunction = udf { array: mutable.WrappedArray[String] =>
      array.map(fullConsentCode => fullConsentCode.split('.')(1)).distinct
    }

    def nih_study_ids: UserDefinedFunction = udf { array: mutable.WrappedArray[String] =>
      array.map(fullConsentCode => fullConsentCode.split('.')(0)).distinct
    }

    def withStudies: DataFrame = {
      val inputColumns: Seq[Column] = df.columns.filterNot(_.equals("studies")).map(col)
      df
        .select(inputColumns :+ explode(col("studies")).as("study_id"):_*)
        .withColumn("full_consent_codes", col("consent_codes_by_study")(col("study_id")))
        .withColumn("short_consent_codes", short_consent_codes_udf(col("full_consent_codes")))
        .withColumn("nih_study_ids", nih_study_ids(col("full_consent_codes")))
        .withColumn("study", struct(
          col("study_id"),
          col("short_consent_codes"),
          col("full_consent_codes"),
          col("nih_study_ids"),
          struct(
            frequenciesByStudiesFor("hmb"),
            frequenciesByStudiesFor("gru")
          ).as("frequencies")))
        .groupBy(locus:_*)
        .agg(
          collect_set("study").as("studies"),
          (inputColumns.toSet -- locus.toSet).map(c => first(c).as(c.toString)).toList:_*)
    }

    def withFrequencies: DataFrame = {
      df
        .withColumn("frequencies", struct(
          struct(
            col("1k_genomes.ac") as "ac",
            col("1k_genomes.an") as "an",
            col("1k_genomes.af") as "af"
          ).as("1k_genomes"),
          struct(
            col("topmed.ac") as "ac",
            col("topmed.an") as "an",
            col("topmed.af") as "af",
            col("topmed.het") as "heterozygotes",
            col("topmed.hom") as "heterozygotes"
          ).as("topmed"),
          frequenciesForGnomad("gnomad_genomes_2_1"),
          frequenciesForGnomad("gnomad_exomes_2_1"),
          frequenciesForGnomad("gnomad_genomes_3_0"),
          struct(
            frequenciesForPrefix("hmb"),
            frequenciesForPrefix("gru")
          ).as("internal")
        ))
    }

    def withClinVar: DataFrame = {
      df
        .withColumn("clinvar", struct(
          col("clinvar_id").as("name"),
          col("clin_sig").as("clin_sig")
        ))
    }

    def withScores: DataFrame = {
      df
        .withColumn("scores",
          struct(
            struct(
              col("SIFT_converted_rankscore") as "sift_converted_rank_score",
              col("SIFT_pred") as "sift_pred",
              col("Polyphen2_HVAR_rankscore") as "polyphen2_hvar_score",
              col("Polyphen2_HVAR_pred") as "polyphen2_hvar_pred",
              col("FATHMM_converted_rankscore"),
              col("FATHMM_pred") as "fathmm_pred",
              col("CADD_raw_rankscore") as "cadd_score",
              col("DANN_rankscore") as "dann_score",
              col("REVEL_rankscore") as "revel_rankscore",
              col("LRT_converted_rankscore") as "lrt_converted_rankscore",
              col("LRT_pred") as "lrt_pred"
            ).as("predictions"),
            struct(
              col("phyloP17way_primate_rankscore") as "phylo_p17way_primate_rankscore"
            ).as("conservations")
          )
        )
    }
  }
}