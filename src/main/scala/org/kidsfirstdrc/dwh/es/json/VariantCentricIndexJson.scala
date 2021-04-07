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
      .read.parquet(s"${Clinical.variants.rootPath}/variants/variants_$releaseId")
      .withColumn("study", explode(col("studies"))).select("study").distinct.as[String].collect()
      .map(studyId =>
        Try(spark.read.parquet(s"${Clinical.occurrences.rootPath}/occurrences/${tableName("occurrences", studyId, releaseId)}")))
      .collect { case Success(df) => df }
      .reduce( (df1, df2) => df1.unionByName(df2))

    Map(
      Clinical.variants     -> spark.read.parquet(s"${Clinical.variants.rootPath}/variants/variants_$releaseId"),
      Clinical.consequences -> spark.read.parquet(s"${Clinical.consequences.rootPath}/consequences/consequences_$releaseId"),
      Clinical.occurrences  -> occurrences,
      Public.clinvar        -> spark.table(s"${Public.clinvar.database}.${Public.clinvar.name}"),
      Public.genes          -> spark.table(s"${Public.genes.database}.${Public.genes.name}")
    )
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val variants = data(Clinical.variants)
      .drop("end")
      .withColumnRenamed("dbsnp_id", "rsnumber")

    val consequences = data(Clinical.consequences)
      .withColumnRenamed("impact", "vep_impact")

    val occurrences = data(Clinical.occurrences).selectLocus(col("participant_id"))

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
      .withParticipants(occurrences)
      .withColumn("locus", concat_ws("-", locus:_*))
      .withColumn("hash", sha1(col("locus")))
      .withColumn("genome_build", lit("GRCh38"))
      .withStudies
      .withFrequencies
      .withClinVar(clinvar)
      .withConsequences(consequences)
      .withGenes(genes)
      .select("genome_build", "hash", "chromosome", "start", "reference", "alternate", "locus", "variant_class",
        "studies", "participant_number", "acls", "external_study_ids", "frequencies", "clinvar", "rsnumber", "release_id",
        "consequences", "impact_score", "genes", "hgvsg", "participant_ids")
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      //avoids many small files created by the following partitionBy() operation
      .repartition(1000, col("chromosome"))
      .write
      .option("maxRecordsPerFile", 200000)
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
      col(s"frequencies.${prefix}.ac") as "ac",
      col(s"frequencies.${prefix}.an") as "an",
      col(s"frequencies.${prefix}.af") as "af",
      col(s"frequencies.${prefix}.homozygotes") as "homozygotes",
      col(s"frequencies.${prefix}.heterozygotes") as "heterozygotes"
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

  private def frequenciesByStudies: Column = {
    struct(
      col(s"ac_by_study")(col("study_id")) as "ac",
      col(s"an_by_study")(col("study_id")) as "an",
      col(s"af_by_study")(col("study_id")) as "af",
      col(s"homozygotes_by_study")(col("study_id")) as "homozygotes",
      col(s"heterozygotes_by_study")(col("study_id")) as "heterozygotes"
    ).as("frequencies")
  }

  implicit class DataFrameOperations(df: DataFrame) {

    def withConsequences(consequences: DataFrame): DataFrame = {

      val consequenceWithScores =
        consequences
          .withScores
          .select("chromosome", "start", "reference", "alternate", "symbol", "ensembl_gene_id", "consequences",
            "vep_impact", "symbol", "strand", "biotype", "exon", "intron", "hgvsc", "hgvsp", "cds_position",
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
            collect_set(col("symbol")) as "symbols",
            max(col("impact_score")) as "impact_score"
          )

      df.joinByLocus(consequencesDf, "left")
    }

    def external_study_ids: UserDefinedFunction = udf { array: mutable.WrappedArray[String] =>
      array.map(fullConsentCode => fullConsentCode.split('.')(0)).distinct
    }

    Some("x").fold(Seq.empty[String])(x => x.split(",").toSeq)

    def withStudies: DataFrame = {
      val inputColumns: Seq[Column] = df.columns.filterNot(_.equals("studies")).map(col)
      df
        .select(inputColumns :+ explode(col("studies")).as("study_id"):_*)
        .withColumn("acls", col("consent_codes_by_study")(col("study_id")))
        .withColumn("external_study_ids", external_study_ids(col("acls")))
        .withColumn("participant_number",
          col("upper_bound_kf_homozygotes_by_study")(col("study_id")) +
            col("upper_bound_kf_heterozygotes_by_study")(col("study_id")))
            //col("gru_homozygotes_by_study")(col("study_id")) +
            //col("gru_heterozygotes_by_study")(col("study_id")))
        .withColumn("study", struct(
          col("study_id"),
          col("acls"),
          col("external_study_ids"),
          //frequenciesByStudies as "frequencies",
          struct(
            frequenciesByStudiesFor("upper_bound_kf"),
            frequenciesByStudiesFor("lower_bound_kf")
          ).as("frequencies"),
          col("participant_number")))
        .filter(col("study.participant_number").isNotNull and col("study.participant_number") > 0)
        .groupBy(locus:_*)
        .agg(
          collect_set("study").as("studies"),
          (inputColumns.toSet -- locus.toSet).map(c => first(c).as(c.toString)).toList :+
            sum(col("participant_number")).as("participant_number"):+
            flatten(collect_set("acls")).as("acls"):+
            flatten(collect_set("external_study_ids")).as("external_study_ids"):_*)

    }

    def withFrequencies: DataFrame = {
      df//.withColumn("internal", col("frequency"))
        //.withCombinedFrequencies("combined", "hmb", "gru")
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
            frequenciesForPrefix("upper_bound_kf"),
            frequenciesForPrefix("lower_bound_kf")
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
              col("SIFT_converted_rankscore") as "sift_converted_rankscore",
              col("SIFT_score") as "sift_score",
              col("SIFT_pred") as "sift_pred",
              col("Polyphen2_HVAR_rankscore") as "polyphen2_hvar_rankscore",
              col("Polyphen2_HVAR_score") as "polyphen2_hvar_score",
              col("Polyphen2_HVAR_pred") as "polyphen2_hvar_pred",
              col("FATHMM_converted_rankscore") as "fathmm_converted_rankscore",
              col("FATHMM_pred") as "fathmm_pred",
              col("CADD_raw_rankscore") as "cadd_rankscore",
              col("DANN_rankscore") as "dann_rankscore",
              col("DANN_score") as "dann_score",
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
          //.where(col("is_gru") || col("is_hmb"))
          .groupByLocus()
          .agg(collect_set(col("participant_id")) as "participant_ids")

      df.joinByLocus(occurrencesWithParticipants, "inner")
        .withColumn("participant_ids", array_remove(col("participant_ids"), ""))
    }
  }
}