package org.kidsfirstdrc.dwh.variantDb.json

import org.apache.spark.sql.functions.{explode, _}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.external.omim.ImportOmimGeneSet
import org.kidsfirstdrc.dwh.join.JoinConsequences
import org.kidsfirstdrc.dwh.utils.MultiSourceEtlJob
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locus
import org.kidsfirstdrc.dwh.variantDb.json.VariantsToJsonJob._
import org.kidsfirstdrc.dwh.vcf.Variants

object VariantsToJsonJob {

  private def frequenciesFor(prefix: String): Column = {
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

    def withStudies: DataFrame = {
      val inputColumns: Seq[Column] = df.columns.filterNot(_.equals("studies")).map(col)
      df
        .select(inputColumns :+ explode(col("studies")).as("study_id"):_*)
        .withColumn("consent_codes", col("consent_codes_by_study")(col("study_id")))
        .withColumn("study", struct(
          col("study_id"),
          col("consent_codes"),
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
          col("one_k_genomes").as("1k_genomes"),
          col("topmed"),
          col("gnomad_genomes_2_1"),
          col("gnomad_exomes_2_1"),
          col("gnomad_genomes_3_0"),
          struct(
            frequenciesFor("hmb"),
            frequenciesFor("gru")
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

class VariantsToJsonJob(releaseId: String) extends MultiSourceEtlJob {

  final val TABLE_NAME = "variant_index"

  override def extract(input: String)(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      Variants.TABLE_NAME -> spark.table(s"variant.${Variants.TABLE_NAME}_$releaseId"),
      JoinConsequences.TABLE_NAME -> spark.table(s"variant.${JoinConsequences.TABLE_NAME}_$releaseId"),
      ImportOmimGeneSet.TABLE_NAME -> spark.table(s"variant.${ImportOmimGeneSet.TABLE_NAME}")
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val variants = data(Variants.TABLE_NAME)

    val consequences = data(JoinConsequences.TABLE_NAME)

    val consequencesColumns = Set("chromosome", "start", "reference", "alternate", "symbol", "ensembl_gene_id", "consequences",
      "name", "impact", "symbol", "strand", "biotype", "variant_class", "exon", "intron", "hgvsc", "hgvsp", "hgvsg",
      "cds_position", "cdna_position", "protein_position", "amino_acids", "codons", "canonical", "aa_change", "coding_dna_change",
      "ensembl_transcript_id", "ensembl_regulatory_id", "feature_type", "scores")

    val scoresColumns = consequences.schema.fields.filter(_.dataType.equals(DoubleType)).map(_.name).toSet

    val omim = data(ImportOmimGeneSet.TABLE_NAME)
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

  override def load(data: DataFrame, output: String)(implicit spark: SparkSession): Unit = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .json(s"$output/tmp/${this.TABLE_NAME}_${this.releaseId}")
  }

}