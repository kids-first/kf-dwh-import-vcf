package org.kidsfirstdrc.dwh.variantDb.json

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{explode, _}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, ElasticsearchJson, Public}
import org.kidsfirstdrc.dwh.conf._
import org.kidsfirstdrc.dwh.jobs.DataSourceEtl
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locus
import org.kidsfirstdrc.dwh.variantDb.json.VariantsToJsonJob._

import scala.collection.mutable

class VariantsToJsonJob(releaseId: String) extends DataSourceEtl(Environment.PROD) {

  override val destination: DataSource = ElasticsearchJson.variantsJson
  val tableName: String = "variant_index"

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(
      Clinical.variants -> spark.table(s"${Clinical.variants.database}.${Clinical.variants.name}"),
      Clinical.consequences -> spark.table(s"${Clinical.consequences.database}.${Clinical.consequences.name}"),
      Public.omim_gene_set -> spark.table(s"${Public.omim_gene_set.database}.${Public.omim_gene_set.name}")
    )
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val variants = data(Clinical.variants)

    val consequences = data(Clinical.consequences)

    val consequencesColumns = Set("chromosome", "start", "reference", "alternate", "symbol", "ensembl_gene_id", "consequences",
      "name", "impact", "symbol", "strand", "biotype", "variant_class", "exon", "intron", "hgvsc", "hgvsp", "hgvsg",
      "cds_position", "cdna_position", "protein_position", "amino_acids", "codons", "canonical", "aa_change", "coding_dna_change",
      "ensembl_transcript_id", "ensembl_regulatory_id", "feature_type", "scores")

    val omim = data(Public.omim_gene_set)
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
      .select("chromosome", "start", "end", "reference", "alternate", "studies", "hmb_participant_number",
        "gru_participant_number", "frequencies", "clinvar", "dbsnp_id", "release_id")
      .joinByLocus(consequencesWithOmim)
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .json(s"${destination.bucket}/es_index/${destination.name}_${this.releaseId}")
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

    def external_study_ids: UserDefinedFunction = udf { array: mutable.WrappedArray[String] =>
      array.map(fullConsentCode => fullConsentCode.split('.')(0)).distinct
    }

    def withStudies: DataFrame = {
      val inputColumns: Seq[Column] = df.columns.filterNot(_.equals("studies")).map(col)
      df
        .select(inputColumns :+ explode(col("studies")).as("study_id"):_*)
        .withColumn("acls", col("consent_codes_by_study")(col("study_id")))
        .withColumn("external_study_ids", external_study_ids(col("acls")))
        .withColumn("hmb_participant_number",
          col("hmb_homozygotes_by_study")(col("study_id")) +
            col("hmb_heterozygotes_by_study")(col("study_id")))
        .withColumn("gru_participant_number",
          col("gru_homozygotes_by_study")(col("study_id")) +
            col("gru_heterozygotes_by_study")(col("study_id")))
        .withColumn("study", struct(
          col("study_id"),
          col("acls"),
          col("external_study_ids"),
          struct(
            frequenciesByStudiesFor("hmb"),
            frequenciesByStudiesFor("gru")
          ).as("frequencies"),
          col("hmb_participant_number"),
          col("gru_participant_number")))
        .groupBy(locus:_*)
        .agg(
          collect_set("study").as("studies"),
          (inputColumns.toSet -- locus.toSet).map(c => first(c).as(c.toString)).toList :+
            sum(col("hmb_participant_number")).as("hmb_participant_number"):+
            sum(col("gru_participant_number")).as("gru_participant_number"):+
            flatten(collect_set("acls")).as("acls"):+
            flatten(collect_set("external_study_ids")).as("external_study_ids"):_*)
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
            col("topmed.heterozygotes") as "heterozygotes",
            col("topmed.homozygotes") as "homozygotes"
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