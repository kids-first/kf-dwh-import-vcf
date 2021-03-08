package org.kidsfirstdrc.dwh.variantDb.json

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{explode, _}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, ElasticsearchJson, Public}
import org.kidsfirstdrc.dwh.conf._
import org.kidsfirstdrc.dwh.jobs.DataSourceEtl
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locus
import org.kidsfirstdrc.dwh.variantDb.json.VariantsToJsonJob._
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._

import scala.collection.mutable

class VariantsToJsonJob(releaseId: String) extends DataSourceEtl(Environment.PROD) {

  override val destination: DataSource = ElasticsearchJson.variantsJson

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(
      Clinical.variants        -> spark.table(s"${Clinical.variants.database}.${Clinical.variants.name}"),
      Clinical.consequences    -> spark.table(s"${Clinical.consequences.database}.${Clinical.consequences.name}"),
      Public.omim_gene_set     -> spark.table(s"${Public.omim_gene_set.database}.${Public.omim_gene_set.name}"),
      Public.orphanet_gene_set -> spark.table(s"${Public.orphanet_gene_set.database}.${Public.orphanet_gene_set.name}"),
      Public.ddd_gene_set      -> spark.table(s"${Public.ddd_gene_set.database}.${Public.ddd_gene_set.name}"),
      Public.cosmic_gene_set   -> spark.table(s"${Public.cosmic_gene_set.database}.${Public.cosmic_gene_set.name}"),
      Public.clinvar           -> spark.table(s"${Public.clinvar.database}.${Public.clinvar.name}")
    )
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val variants = data(Clinical.variants)
      .withColumnRenamed("dbsnp_id", "rsnumber")

    val consequences = data(Clinical.consequences)
      .withColumnRenamed("impact", "vep_impact")

    val omim = data(Public.omim_gene_set)
      .select("ensembl_gene_id", "entrez_gene_id", "omim_gene_id")

    val ddd_gene_set = data(Public.ddd_gene_set)
      .select("disease_name", "symbol")

    val cosmic_gene_set = data(Public.cosmic_gene_set)
      .select("symbol", "tumour_types_germline")

    val clinvar = data(Public.clinvar)
      .selectLocus(
        col("name") as "clinvar_id",
        col("clin_sig"),
        col("conditions"),
        col("inheritance"),
        col("interpretations"))


    val orphanet = data(Public.orphanet_gene_set)
      .select(
        col("gene_symbol").as("symbol"),
        col("disorder_id").as("orphanet_disorder_id"),
        col("name").as("panel"),
        col("type_of_inheritance").as("inheritance"))

    variants
      .withColumn("locus", concat_ws("-", locus:_*))
      .withStudies
      .withFrequencies
      .withClinVar(clinvar)
      .withConsequences(consequences, omim, orphanet, ddd_gene_set, cosmic_gene_set)
      .select("chromosome", "start", "end", "reference", "alternate", "locus", "studies", "participant_number",
        "acls", "external_study_ids", "frequencies", "clinvar", "rsnumber", "release_id", "consequences",
        "orphanet_disorder_ids", "panels", "inheritances", "hgvsg", "disease_names", "tumour_types_germlines",
        "omim_gene_ids", "entrez_gene_ids", "ensembl_gene_ids")
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .format("json")
      .json(s"${destination.bucket}/es_index/${destination.name}_${this.releaseId}")
    data
  }

  override def publish(view_db: String)(implicit spark: SparkSession): Unit = {
    //no publish
  }

  override def run()(implicit spark: SparkSession): DataFrame = {
    val inputDF = extract()
    val outputDF = transform(inputDF)
    load(outputDF)
    outputDF
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

    def withConsequences(consequences: DataFrame, omim: DataFrame, orphanet: DataFrame, ddd_gene_set: DataFrame,
                         cosmic_gene_set: DataFrame): DataFrame = {

      val consequenceWithScores =
        consequences
          .withScores
          .select("chromosome", "start", "reference", "alternate", "symbol", "ensembl_gene_id", "consequences",
            "vep_impact", "symbol", "strand", "biotype", "variant_class", "exon", "intron", "hgvsc", "hgvsp", "cds_position",
            "cdna_position", "protein_position", "amino_acids", "codons", "canonical", "aa_change", "coding_dna_change",
            "ensembl_transcript_id", "ensembl_regulatory_id", "feature_type", "predictions", "conservations")

      val columnsAtGeneLevel: Set[String] =
        orphanet.columns.toSet ++
          omim.columns.toSet ++
          ddd_gene_set.columns.toSet ++
          cosmic_gene_set.columns.toSet --
          Set("inheritance", "tumour_types_germline", "symbol")

      val consequenceOutputColumns: Set[String] =
        consequenceWithScores.columns.toSet ++
          Set("impact_score") --
          columnsAtGeneLevel --
          Set("chromosome", "start", "reference", "alternate")

      val consequencesDf =
        consequenceWithScores
          .join(omim, Seq("ensembl_gene_id"), "left")
          .join(orphanet, Seq("symbol"), "left")
          .join(ddd_gene_set, Seq("symbol"), "left")
          .join(cosmic_gene_set, Seq("symbol"), "left")
          .withColumn("impact_score",
            when(col("vep_impact") === "MODIFIER", 1)
              .when(col("vep_impact") === "LOW", 2)
              .when(col("vep_impact") === "MODERATE", 3)
              .when(col("vep_impact") === "HIGH", 4)
              .otherwise(0))
          .withColumn("consequence", struct(consequenceOutputColumns.toSeq.map(col):_*))
          .groupBy(locus:_*)
          .agg(
            collect_set(col("consequence")).as("consequences"),
            columnsAtGeneLevel.map(c => collect_set(col(c)).as(s"${c}s")).toSeq :+
              flatten(collect_set(col("inheritance"))).as("inheritances") :+
              flatten(collect_set(col("tumour_types_germline"))).as("tumour_types_germlines"):_*
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
  }
}