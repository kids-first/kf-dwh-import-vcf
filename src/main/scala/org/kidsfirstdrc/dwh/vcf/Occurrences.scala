package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.utils.ClinicalUtils.{getBiospecimens, getRelations}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object Occurrences {

  def run(studyId: String, releaseId: String, input: String, output: String, biospecimenIdColumn: String, isPostCGPOnly: Boolean)(implicit spark: SparkSession): Unit = {
    write(build(studyId, releaseId, input, biospecimenIdColumn, isPostCGPOnly), output, studyId, releaseId)
  }

  def build(studyId: String, releaseId: String, input: String, biospecimenIdColumn: String, isPostCGPOnly: Boolean)(implicit spark: SparkSession): DataFrame = {
    val occurrences = selectOccurrences(studyId, releaseId, input, isPostCGPOnly)
    val biospecimens = getBiospecimens(studyId, releaseId, biospecimenIdColumn)
    val withClinical = joinOccurrencesWithClinical(occurrences, biospecimens)

    val relations = getRelations(studyId, releaseId)
    joinOccurrencesWithInheritence(withClinical, relations)
  }

  def selectOccurrences(studyId: String, releaseId: String, input: String, isPostCGPOnly: Boolean)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val inputDF: DataFrame = if (isPostCGPOnly) loadPostCGP(input, studyId, releaseId) else unionCGPFiles(input, studyId, releaseId)

    val occurrences = inputDF
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        firstAnn,
        $"genotype.sampleId" as "biospecimen_id",
        $"genotype.alleleDepths" as "ad",
        $"genotype.depth" as "dp",
        $"genotype.conditionalQuality" as "gq",
        $"genotype.calls" as "calls",
        has_alt,
        is_multi_allelic,
        old_multi_allelic,
        $"qual" as "quality",
        $"INFO_filters"(0) as "filter",
        ac as "info_ac",
        an as "info_an",
        $"INFO_AF"(0) as "info_af",
        $"INFO_culprit" as "info_culprit",
        $"INFO_SOR" as "info_sor",
        $"INFO_ReadPosRankSum" as "info_read_pos_rank_sum",
        $"INFO_InbreedingCoeff" as "info_inbreeding_coeff",
        $"INFO_PG" as "info_pg",
        $"INFO_FS" as "info_fs",
        $"INFO_DP" as "info_dp",
        optional_info(inputDF, "INFO_DS", "info_ds", "boolean"),
        $"INFO_NEGATIVE_TRAIN_SITE" as "info_info_negative_train_site",
        $"INFO_POSITIVE_TRAIN_SITE" as "info_positive_train_site",
        $"INFO_VQSLOD" as "info_vqslod",
        $"INFO_ClippingRankSum" as "info_clipping_rank_sum",
        $"INFO_RAW_MQ" as "info_raw_mq",
        $"INFO_BaseQRankSum" as "info_base_qrank_sum",
        $"INFO_MLEAF"(0) as "info_mleaf",
        $"INFO_MLEAC"(0) as "info_mleac",
        $"INFO_MQ" as "info_mq",
        $"INFO_QD" as "info_qd",
        $"INFO_DB" as "info_db",
        $"INFO_MQRankSum" as "info_m_qrank_sum",
        optional_info(inputDF, "INFO_loConfDeNovo", "lo_conf_denovo"),
        optional_info(inputDF, "INFO_hiConfDeNovo", "hi_conf_denovo"),
        $"INFO_ExcessHet" as "info_excess_het",
        optional_info(inputDF, "INFO_HaplotypeScore", "info_haplotype_score", "float"),
        $"file_name",
        lit(studyId) as "study_id",
        lit(releaseId) as "release_id"
      )
      .withColumn("is_lo_conf_denovo", array_contains(split($"lo_conf_denovo", ","), $"biospecimen_id"))
      .withColumn("is_hi_conf_denovo", array_contains(split($"hi_conf_denovo", ","), $"biospecimen_id"))
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .drop("annotation", "lo_conf_denovo", "hi_conf_denovo")
    occurrences
  }

  /**
   * This function is a hack for loading both CGP and postCGP files.
   * These 2 kinds of vcf does not have the same headers, so it results in error when trying to parse these files together
   * That's why we need to address schema differences, and then union these 2 dtafremaes manually.
   *
   * @param input path where are located the files
   * @param studyId
   * @param releaseId
   * @param spark
   * @return a dataframe that unions cgp and postcgp vcf
   */
  private def unionCGPFiles(input: String, studyId: String, releaseId: String)(implicit spark: SparkSession) = {
    (postCGPExist(input), cgpExist(input)) match {
      case (false, true) => loadCGP(input, studyId, releaseId)
      case (true, false) => loadPostCGP(input, studyId, releaseId)
      case (true, true) =>
        val postCGP = loadPostCGP(input, studyId, releaseId)
        val cgp = loadCGP(input, studyId, releaseId)
        union(postCGP, cgp)
      case (false, false) => throw new IllegalStateException("No VCF files found!")
    }

  }

  private def loadPostCGP(input: String, studyId: String, releaseId: String)(implicit spark: SparkSession) = {
    import spark.implicits._
    visibleVcf(postCGPFiles(input), studyId, releaseId)
      .withColumn("genotype", explode($"genotypes"))
  }

  private def loadCGP(input: String, studyId: String, releaseId: String)(implicit spark: SparkSession) = {
    import spark.implicits._
    visibleVcf(cgpFiles(input), studyId, releaseId)
      .withColumn("INFO_DS", lit(null).cast("boolean"))
      .withColumn("INFO_HaplotypeScore", lit(null).cast("double"))
      .withColumn("genotype", explode($"genotypes"))
      .drop("genotypes")
      .withColumn("INFO_ReadPosRankSum", $"INFO_ReadPosRankSum"(0))
      .withColumn("INFO_ClippingRankSum", $"INFO_ClippingRankSum"(0))
      .withColumn("INFO_RAW_MQ", $"INFO_RAW_MQ"(0))
      .withColumn("INFO_BaseQRankSum", $"INFO_BaseQRankSum"(0))
      .withColumn("INFO_MQRankSum", $"INFO_MQRankSum"(0))
      .withColumn("INFO_ExcessHet", $"INFO_ExcessHet"(0))
      .withColumn("genotype", struct(
        $"genotype.sampleId",
        $"genotype.conditionalQuality",
        $"genotype.filters",
        $"genotype.SB",
        $"genotype.alleleDepths",
        $"genotype.PP",
        $"genotype.PID"(0) as "PID",
        $"genotype.phased",
        $"genotype.calls",
        $"genotype.MIN_DP"(0) as "MIN_DP",
        $"genotype.JL",
        $"genotype.PGT"(0) as "PGT",
        $"genotype.phredLikelihoods",
        $"genotype.depth",
        $"genotype.RGQ",
        $"genotype.JP"
      ))
  }

  def joinOccurrencesWithInheritence(occurrences: DataFrame, relations: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    occurrences.join(relations, occurrences("participant_id") === relations("participant_id"), "left")
      .drop(relations("participant_id"))
      .withColumn("family_calls", familyCalls)
      .withColumn("mother_calls", $"family_calls"($"mother_id"))
      .withColumn("father_calls", $"family_calls"($"father_id"))
      .drop("family_calls")
      .withColumn("zygosity", zygosity($"calls"))
      .withColumn("mother_zygosity", zygosity($"mother_calls"))
      .withColumn("father_zygosity", zygosity($"father_calls"))
  }

  def joinOccurrencesWithClinical(occurrences: DataFrame, biospecimens: DataFrame)(implicit spark: SparkSession): DataFrame = {
    occurrences
      .join(biospecimens, occurrences("biospecimen_id") === biospecimens("joined_sample_id"))
      .drop(occurrences("biospecimen_id")).drop(biospecimens("joined_sample_id"))
  }


  def write(df: DataFrame, output: String, studyId: String, releaseId: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val tableOccurence = tableName("occurrences", studyId, releaseId)
    df
      .repartitionByRange(700, $"has_alt", $"dbgap_consent_code", $"chromosome", $"start")
      .write.mode("overwrite")
      .partitionBy("study_id", "has_alt", "dbgap_consent_code", "chromosome")
      .format("parquet")
      .option("path", s"$output/occurrences/$tableOccurence")
      .saveAsTable(tableOccurence)
  }

}
