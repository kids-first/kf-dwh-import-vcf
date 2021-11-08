package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RunType}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locus
import bio.ferlab.datalake.spark3.implicits.SparkUtils._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Es, Public, Raw}
import org.kidsfirstdrc.dwh.es.index.VariantCentricIndex._
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._

import java.time.LocalDateTime
import scala.collection.mutable

class VariantCentricIndex(schema: String, releaseId: String)(implicit conf: Configuration) extends ETL()(conf) {

  override val destination: DatasetConf = Es.variant_centric

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      Clinical.variants.id      -> spark.read.parquet(s"${Clinical.variants.rootPath}/variants/variants_$releaseId"),
      Clinical.consequences.id  -> spark.read.parquet(s"${Clinical.consequences.rootPath}/consequences/consequences_$releaseId"),
      Clinical.occurrences.id   -> getOccurrencesWithAlt(schema, releaseId),
      Public.clinvar.id         -> spark.table(s"${Public.clinvar.table.get.fullName}"),
      Public.genes.id           -> spark.table(s"${Public.genes.table.get.fullName}"),
      Raw.studies_short_name.id -> spark.read.options(Raw.studies_short_name.readoptions).csv(Raw.studies_short_name.location)
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val variants = data(Clinical.variants.id)
      .drop("end")
      .where(col("hgvsg").isNotNull) //SKFP-190
      .withColumn("zygosity", sort_array(filter(col("zygosity"), c => c.isin("HOM", "HET", "UNK")))) //SKFP-192
      .withColumnRenamed("dbsnp_id", "rsnumber")

    val consequences = data(Clinical.consequences.id)
      .withColumnRenamed("impact", "vep_impact")

    val occurrences = data(Clinical.occurrences.id)
      .selectLocus(col("participant_id"), col("study_id"))

    val clinvar = data(Public.clinvar.id)
      .selectLocus(
        col("name") as "clinvar_id",
        col("clin_sig"),
        col("conditions"),
        col("inheritance"),
        col("interpretations")
      )

    val genes = data(Public.genes.id)
      .drop("biotype")
      .withColumnRenamed("chromosome", "genes_chromosome")

    val studyCodes = data(Raw.studies_short_name.id)
      .select(
        col("kf_id") as "study_id",
        col("code") as "study_code"
      )

    variants
      .withParticipants(occurrences)
      .withColumn("locus", concat_ws("-", locus: _*))
      .withColumn("hash", sha1(col("locus")))
      .withColumn("genome_build", lit("GRCh38"))
      .withStudies(studyCodes)
      .withColumn(
        "participant_total_number",
        (col("frequencies.upper_bound_kf.an") / 2).cast(LongType)
      )
      .withColumn(
        "participant_frequency",
        (col("participant_number") / col("participant_total_number")).cast(DoubleType)
      )
      .withFrequencies
      .withClinVar(clinvar)
      .withConsequences(consequences)
      .withGenes(genes)
      .withGeneExternalReference
      .withVariantExternalReference
      .withTransmissions
      .select(
        "genome_build",
        "hash",
        "chromosome",
        "start",
        "reference",
        "alternate",
        "locus",
        "variant_class",
        "studies",
        "participant_number",
        "participant_number_visible",
        "acls",
        "external_study_ids",
        "frequencies",
        "clinvar",
        "rsnumber",
        "release_id",
        "consequences",
        "vep_impacts",
        "max_impact_score",
        "genes",
        "hgvsg",
        "participant_total_number",
        "participant_frequency",
        "gene_external_reference",
        "variant_external_reference",
        "transmissions",
        "zygosity"
      )
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    data
      //avoids many small files created by the following partitionBy() operation
      .repartition(1000, col("chromosome"))
      .write
      .option("maxRecordsPerFile", 200000)
      .partitionBy(destination.partitionby:_*)
      .mode(SaveMode.Overwrite)
      .option("format", destination.format.sparkFormat)
      .option("path", s"${destination.rootPath}/es_index/${destination.id}_${releaseId}")
      .saveAsTable(s"${destination.table.get.fullName}_${releaseId}")
    data
  }

  override def run(runType: RunType)(implicit spark: SparkSession): DataFrame = {
    val inputDF  = extract()
    val outputDF = transform(inputDF).persist()
    println(s"count: ${outputDF.count}")
    load(outputDF)
    outputDF
  }
}

object VariantCentricIndex {

  /**
   * Minimum number of participant having a trait in order to be publicly visible in the 'variant_centric' index
   */
  final val minimumParticipantsPerStudy = 10

  val transmissionsToKeep = List(
    "autosomal_dominant", "autosomal_recessive", "autosomal_dominant_de_novo",
    "x_linked_dominant_de_novo", "x_linked_recessive_de_novo", "x_linked_dominant", "x_linked_recessive")

  private def frequenciesForGnomad(colName: String): Column = {
    struct(
      col(s"$colName.ac") as "ac",
      col(s"$colName.an") as "an",
      col(s"$colName.af").cast(DoubleType) as "af",
      col(s"$colName.hom") as "homozygotes"
    ).as(colName)
  }

  private def frequenciesForPrefix(prefix: String): Column = {
    struct(
      col(s"frequencies.${prefix}.ac") as "ac",
      col(s"frequencies.${prefix}.an") as "an",
      col(s"frequencies.${prefix}.af").cast(DoubleType) as "af",
      col(s"frequencies.${prefix}.homozygotes") as "homozygotes",
      col(s"frequencies.${prefix}.heterozygotes") as "heterozygotes"
    ).as(prefix)
  }

  private def frequenciesByStudiesFor(prefix: String): Column = {
    struct(
      col(s"${prefix}_ac_by_study")(col("study_id")) as "ac",
      col(s"${prefix}_an_by_study")(col("study_id")) as "an",
      col(s"${prefix}_af_by_study")(col("study_id")).cast(DoubleType) as "af",
      col(s"${prefix}_homozygotes_by_study")(col("study_id")) as "homozygotes",
      col(s"${prefix}_heterozygotes_by_study")(col("study_id")) as "heterozygotes"
    ).as(prefix)
  }

  private def frequenciesByStudies: Column = {
    struct(
      col(s"ac_by_study")(col("study_id")) as "ac",
      col(s"an_by_study")(col("study_id")) as "an",
      col(s"af_by_study")(col("study_id")).cast(DoubleType) as "af",
      col(s"homozygotes_by_study")(col("study_id")) as "homozygotes",
      col(s"heterozygotes_by_study")(col("study_id")) as "heterozygotes"
    ).as("frequencies")
  }

  implicit class DataFrameOperations(df: DataFrame) {

    def withConsequences(consequences: DataFrame): DataFrame = {

      val consequenceWithScores =
        consequences.withScores
          .select(
            "chromosome",
            "start",
            "reference",
            "alternate",
            "symbol",
            "ensembl_gene_id",
            "consequences",
            "vep_impact",
            "symbol",
            "strand",
            "biotype",
            "exon",
            "intron",
            "hgvsc",
            "hgvsp",
            "cds_position",
            "cdna_position",
            "protein_position",
            "amino_acids",
            "codons",
            "canonical",
            "mane_plus",
            "mane_select",
            "aa_change",
            "refseq_mrna_id",
            "refseq_protein_id",
            "coding_dna_change",
            "ensembl_transcript_id",
            "ensembl_regulatory_id",
            "feature_type",
            "predictions",
            "conservations"
          )

      val consequenceOutputColumns: Set[String] =
        consequenceWithScores.columns.toSet ++ Set("impact_score") -- Set(
          "chromosome",
          "start",
          "reference",
          "alternate"
        )

      val consequencesDf =
        consequenceWithScores
          .withColumn(
            "impact_score",
            when(col("vep_impact") === "MODIFIER", 1)
              .when(col("vep_impact") === "LOW", 2)
              .when(col("vep_impact") === "MODERATE", 3)
              .when(col("vep_impact") === "HIGH", 4)
              .otherwise(0)
          )
          .withColumn("consequence", struct(consequenceOutputColumns.toSeq.map(col): _*))
          .groupBy(locus: _*)
          .agg(
            collect_set(col("consequence")) as "consequences",
            collect_set(col("symbol")) as "symbols",
            collect_set(col("vep_impact")) as "vep_impacts",
            max(col("impact_score")) as "max_impact_score"
          )

      df.joinByLocus(consequencesDf, "left")
    }

    def external_study_ids: UserDefinedFunction = udf { array: mutable.WrappedArray[String] =>
      array.map(fullConsentCode => fullConsentCode.split('.')(0)).distinct
    }

    def withStudies(studyCodes: DataFrame): DataFrame = {
      val inputColumns: Seq[Column] = df.columns.filterNot(_.equals("studies")).map(col)
      df
        .select(inputColumns :+ explode(col("studies")).as("study_id"): _*)
        .join(studyCodes, Seq("study_id"), "left")
        .withColumn("acls", col("consent_codes_by_study")(col("study_id")))
        .withColumn("external_study_ids", external_study_ids(col("acls")))
        .withColumn("ids", col("participant_ids_by_study")(col("study_id")))
        .withColumn(
          "participant_number",
          when(col("ids").isNull, lit(0))
            .otherwise(size(col("ids")))
        )
        .withColumn(
          "participant_number_visible",
          when(size(col("ids")) >= minimumParticipantsPerStudy, size(col("ids"))).otherwise(lit(0))
        )
        .withColumn(
          "participant_ids",
          when(size(col("ids")) >= minimumParticipantsPerStudy, col("ids")).otherwise(lit(null))
        )
        .withColumn("transmission_by_study",
          filter(
            map_keys(col("transmissions_by_study")(col("study_id"))), _.isin(transmissionsToKeep:_*)
          )
        )
        .withColumn(
          "study",
          struct(
            col("study_id"),
            col("study_code"),
            col("acls"),
            col("external_study_ids"),
            col("transmission_by_study") as "transmissions",
            struct(
              frequenciesByStudiesFor("upper_bound_kf"),
              frequenciesByStudiesFor("lower_bound_kf")
            ).as("frequencies"),
            col("participant_number"),
            col("participant_ids")
          )
        )
        .groupBy(locus: _*)
        .agg(
          collect_list("study").as("studies"),
          (inputColumns.toSet -- locus.toSet).map(c => first(c).as(c.toString)).toList :+
            sum(col("participant_number_visible")).as("participant_number_visible") :+
            sum(col("participant_number")).as("participant_number") :+
            flatten(collect_set("acls")).as("acls") :+
            flatten(collect_set("external_study_ids")).as("external_study_ids"): _*
        )

    }

    def withFrequencies: DataFrame = {
      df.withColumn(
        "frequencies",
        struct(
          struct(
            col("1k_genomes.ac") as "ac",
            col("1k_genomes.an") as "an",
            col("1k_genomes.af").cast(DoubleType) as "af"
          ).as("one_thousand_genomes"),
          struct(
            col("topmed.ac") as "ac",
            col("topmed.an") as "an",
            col("topmed.af").cast(DoubleType) as "af",
            col("topmed.heterozygotes") as "heterozygotes",
            col("topmed.homozygotes") as "homozygotes"
          ).as("topmed"),
          frequenciesForGnomad("gnomad_genomes_2_1"),
          frequenciesForGnomad("gnomad_exomes_2_1"),
          frequenciesForGnomad("gnomad_genomes_3_0"),
          frequenciesForGnomad("gnomad_genomes_3_1_1"),
          struct(
            frequenciesForPrefix("upper_bound_kf"),
            frequenciesForPrefix("lower_bound_kf")
          ).as("internal")
        )
      )
    }

    def withClinVar(clinvar: DataFrame): DataFrame = {
      df.drop("clinvar_id", "clin_sig")
        .joinAndMerge(clinvar, "clinvar", "left")
    }

    def withScores: DataFrame = {
      df
        .withColumn(
          "predictions",
          struct(
            col("SIFT_converted_rankscore") as "sift_converted_rankscore",
            col("SIFT_pred") as "sift_pred",
            col("Polyphen2_HVAR_rankscore") as "polyphen2_hvar_rankscore",
            col("Polyphen2_HVAR_pred") as "polyphen2_hvar_pred",
            col("FATHMM_converted_rankscore") as "fathmm_converted_rankscore",
            col("FATHMM_pred") as "fathmm_pred",
            col("CADD_raw_rankscore") as "cadd_rankscore",
            col("DANN_rankscore") as "dann_rankscore",
            col("REVEL_rankscore") as "revel_rankscore",
            col("LRT_converted_rankscore") as "lrt_converted_rankscore",
            col("LRT_pred") as "lrt_pred"
          )
        )
        .withColumn(
          "conservations",
          struct(col("phyloP17way_primate_rankscore") as "phylo_p17way_primate_rankscore")
        )
    }

    def withGenes(genes: DataFrame)(implicit spark: SparkSession): DataFrame = {
      df
        .join(
          broadcast(genes),
          col("chromosome") === col("genes_chromosome") &&
            array_contains(df("symbols"), genes("symbol")),
          "left"
        )
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
          .groupBy(
            col("chromosome"),
            col("start"),
            col("reference"),
            col("alternate"),
            col("study_id")
          )
          .agg(
            collect_list(col("participant_id")) as "participant_ids"
          )
          .groupByLocus()
          .agg(
            map_from_entries(
              collect_list(struct(col("study_id"), col("participant_ids")))
            ) as "participant_ids_by_study"
          )

      df.joinByLocus(occurrencesWithParticipants, "left")
    }

    def withZigozity(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val conditionValueMap: List[(Column, String)] = List(
        $"clinvar".isNotNull -> "Clinvar",
        exists($"genes", gene => gene("hpo").isNotNull and size(gene("hpo")) > 0) -> "HPO",
        exists($"genes", gene => gene("orphanet").isNotNull and size(gene("orphanet")) > 0) -> "Orphanet",
        exists($"genes", gene => gene("omim").isNotNull and size(gene("omim")) > 0) -> "OMIM",
        exists($"genes", gene => gene("cosmic").isNotNull and size(gene("cosmic")) > 0) -> "Cosmic",
        exists($"genes", gene => gene("ddd").isNotNull and size(gene("ddd")) > 0) -> "DDD"
      )
      conditionValueMap.foldLeft {
        df.withColumn("external_reference", when($"rsnumber".isNotNull, array(lit("DBSNP"))).otherwise(array()))
      } { case (d, (condition, value)) => d
        .withColumn("external_reference",
          when(condition, array_union($"external_reference", array(lit(value))))
            .otherwise($"external_reference"))
      }
    }

    def withGeneExternalReference(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val outputColumnName = "gene_external_reference"
      val conditionValueMap: List[(Column, String)] = List(
        exists($"genes", gene => gene("orphanet").isNotNull and size(gene("orphanet")) > 0) -> "Orphanet",
        exists($"genes", gene => gene("omim").isNotNull and size(gene("omim")) > 0) -> "OMIM",
        exists($"genes", gene => gene("cosmic").isNotNull and size(gene("cosmic")) > 0) -> "Cosmic",
        exists($"genes", gene => gene("ddd").isNotNull and size(gene("ddd")) > 0) -> "DDD"
      )
      conditionValueMap.foldLeft {
        df.withColumn(outputColumnName, when(
          exists($"genes", gene => gene("hpo").isNotNull and size(gene("hpo")) > 0), array(lit("HPO"))).otherwise(array()))
      } { case (d, (condition, value)) => d
        .withColumn(outputColumnName,
          when(condition, array_union(col(outputColumnName), array(lit(value)))).otherwise(col(outputColumnName)))
      }
    }

    def withVariantExternalReference(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val outputColumnName = "variant_external_reference"
      val conditionValueMap: List[(Column, String)] = List(
        $"clinvar".isNotNull -> "Clinvar"
      )
      conditionValueMap.foldLeft {
        df.withColumn(outputColumnName, when($"rsnumber".isNotNull, array(lit("DBSNP"))).otherwise(array()))
      } { case (d, (condition, value)) => d
        .withColumn(outputColumnName,
          when(condition, array_union(col(outputColumnName), array(lit(value)))).otherwise(col(outputColumnName)))
      }
    }

    def withTransmissions: DataFrame = {
      df
        .withColumn("transmissions", filter(map_keys(col("transmissions")),  _.isin(transmissionsToKeep:_*)))
    }
  }
}
