package org.kidsfirstdrc.dwh.es.json

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.{DataSource, ETL}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{explode, _}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, DataService, Es, Public}
import org.kidsfirstdrc.dwh.es.json.VariantCentricIndexJson._
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locus

import scala.collection.mutable
import scala.util.{Success, Try}

class VariantCentricIndexJson(releaseId: String)(implicit conf: Configuration)
  extends ETL(Es.variant_centric)(conf) {

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    import spark.implicits._
    val occurrences: DataFrame = spark
      .table(s"${DataService.studies.database}.${DataService.studies.name}")
      .select("study_id")
      .as[String].collect()
      .map(study_id => Try(spark.table(s"${Clinical.occurrences.database}.${Clinical.occurrences.name}_${study_id.toLowerCase}")))
      .collect { case Success(df) => df }
      .reduce( (df1, df2) => df1.unionByName(df2))

    Map(
      Clinical.variants        -> spark.table(s"${Clinical.variants.database}.${Clinical.variants.name}"),
      Clinical.consequences    -> spark.table(s"${Clinical.consequences.database}.${Clinical.consequences.name}"),
      Clinical.occurrences     -> occurrences,
      Public.clinvar           -> spark.table(s"${Public.clinvar.database}.${Public.clinvar.name}"),
      Public.genes             -> spark.table(s"${Public.genes.database}.${Public.genes.name}")
    )
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val variants = data(Clinical.variants)
      .drop("end")
      .withColumnRenamed("dbsnp_id", "rsnumber")

    val consequences = data(Clinical.consequences)
      .withColumnRenamed("impact", "vep_impact")

    val occurrences = data(Clinical.occurrences)
      .selectLocus(col("participant_id"), col("is_hmb"), col("is_gru"))

    val clinvar = data(Public.clinvar)
      .selectLocus(
        col("name") as "clinvar_id",
        col("clin_sig"),
        col("conditions"),
        col("inheritance"),
        col("interpretations"))

    val genes = data(Public.genes).drop("biotype")
      .withColumnRenamed("chromosome", "genes_chromosome")

    variants
      .withColumn("locus", concat_ws("-", locus:_*))
      .withColumn("hash", sha1(col("locus")))
      .withStudies
      .withFrequencies
      .withClinVar(clinvar)
      .withConsequences(consequences)
      .withGenes(genes)
      .withParticipants(occurrences)
      .select("hash", "chromosome", "start", "reference", "alternate", "locus", "studies", "participant_number",
        "acls", "external_study_ids", "frequencies", "clinvar", "rsnumber", "release_id", "consequences", "genes", "hgvsg",
        "participants")
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      .write
      .partitionBy("chromosome")
      .mode(SaveMode.Overwrite)
      .format("json")
      .json(s"${destination.rootPath}/es_index/${destination.name}_$releaseId")
    data
  }

  override def run()(implicit spark: SparkSession): DataFrame = {
    val inputDF = extract()
    val outputDF = transform(inputDF).persist()
    println(s"count: ${outputDF.count}")
    println(s"distinct locus: ${outputDF.dropDuplicates("locus").count()}")
    load(outputDF)
    outputDF
  }
}

object VariantCentricIndexJson {

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

    def withConsequences(consequences: DataFrame): DataFrame = {

      val consequenceWithScores =
        consequences
          .withScores
          .select("chromosome", "start", "reference", "alternate", "symbol", "ensembl_gene_id", "consequences",
            "vep_impact", "symbol", "strand", "biotype", "variant_class", "exon", "intron", "hgvsc", "hgvsp", "cds_position",
            "cdna_position", "protein_position", "amino_acids", "codons", "canonical", "aa_change", "coding_dna_change",
            "ensembl_transcript_id", "ensembl_regulatory_id", "feature_type", "predictions", "conservations")

      val consequenceOutputColumns: Set[String] =
        consequenceWithScores.columns.toSet ++ Set("impact_score") -- Set("chromosome", "start", "reference", "alternate")

      val consequencesDf =
        consequenceWithScores
          .withColumn("impact_score",
            when(col("vep_impact") === "MODIFIER", 1)
              .when(col("vep_impact") === "LOW", 2)
              .when(col("vep_impact") === "MODERATE", 3)
              .when(col("vep_impact") === "HIGH", 4)
              .otherwise(0))
          .withColumn("consequence", struct(consequenceOutputColumns.toSeq.map(col):_*))
          .groupBy(locus:_*)
          .agg(
            collect_set(col("consequence")) as "consequences",
              collect_set(col("symbol")) as "symbols"
          )

      df.joinByLocus(consequencesDf, "left")
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
        .withColumn("participant_number",
          col("hmb_homozygotes_by_study")(col("study_id")) +
            col("hmb_heterozygotes_by_study")(col("study_id")) +
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
          col("participant_number")))
        .groupBy(locus:_*)
        .agg(
          collect_set("study").as("studies"),
          (inputColumns.toSet -- locus.toSet).map(c => first(c).as(c.toString)).toList :+
            sum(col("participant_number")).as("participant_number"):+
            flatten(collect_set("acls")).as("acls"):+
            flatten(collect_set("external_study_ids")).as("external_study_ids"):_*)
    }

    def withFrequencies: DataFrame = {
      df.withCombinedFrequencies("combined", "hmb", "gru")
        .withColumn("frequencies", struct(
          struct(
            col("1k_genomes.ac") as "ac",
            col("1k_genomes.an") as "an",
            col("1k_genomes.af") as "af"
          ).as("one_thousand_genomes"),
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
            frequenciesForPrefix("combined"),
            frequenciesForPrefix("hmb"),
            frequenciesForPrefix("gru")
          ).as("internal")
        ))
    }

    def withClinVar(clinvar: DataFrame): DataFrame = {
      df.drop("clinvar_id", "clin_sig")
        .joinAndMerge(clinvar, "clinvar", "left")
    }

    def withScores: DataFrame = {
      df
        .withColumn("predictions",
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
            ))
        .withColumn("conservations",
            struct(
              col("phyloP17way_primate_rankscore") as "phylo_p17way_primate_rankscore")
        )
    }

    def withGenes(genes: DataFrame)(implicit spark: SparkSession): DataFrame = {
      df
        .join(broadcast(genes), col("chromosome") === col("genes_chromosome") &&
          array_contains(df("symbols"), genes("symbol")), "left")
        .drop("genes_chromosome")
        .groupByLocus()
        .agg(
          first(struct(df("*"))) as "variant",
          collect_list(struct(genes.drop("genes_chromosome")("*"))) as "genes"
        )
        .select("variant.*", "genes")
        .withColumn("genes", removeEmptyObjectsIn("genes"))
    }

    def withParticipants(occurrences: DataFrame)(implicit spark: SparkSession): DataFrame = {

      val occurrencesWithParticipants =
        occurrences
          .where(col("is_gru") || col("is_hmb"))
          .groupByLocus()
          .agg(collect_list(struct(col("participant_id") as "participant_id")) as "participants")

      df.joinByLocus(occurrencesWithParticipants, "left")
        .withColumn("participants", removeEmptyObjectsIn("participants"))
    }
  }
}