package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession, functions}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object Occurrences {

  val filename: Column = regexp_extract(input_file_name(), ".*/(.*)", 1)

  val filterAcl: UserDefinedFunction = udf { acl: Seq[String] =>
    if (acl == null) None else {
      Some(acl.filter(a => a.contains(".") || a == "*"))
    }
  }

  def run(studyId: String, releaseId: String, input: String, output: String, biospecimenIdColumn: String)(implicit spark: SparkSession): Unit = {
    write(build(studyId, releaseId, input, biospecimenIdColumn), output, studyId, releaseId)
  }

  def build(studyId: String, releaseId: String, input: String, biospecimenIdColumn: String)(implicit spark: SparkSession): DataFrame = {
    val occurrences = selectOccurrences(studyId, releaseId, input)
    val biospecimens = getBiospecimens(studyId, releaseId, biospecimenIdColumn)
    val genomicFiles = getGenomicFiles(studyId, releaseId)
    val withClinical = joinOccurrencesWithClinical(occurrences, biospecimens, genomicFiles)

    val relations = getRelations(studyId, releaseId)
    joinOccurrencesWithInheritence(withClinical, relations)
  }

  def selectOccurrences(studyId: String, releaseId: String, input: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val inputDF = vcf(input)
      .withColumn("file_name", filename)
      .withColumn("genotype", explode($"genotypes"))
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
        array_contains($"genotype.calls", 1) as "has_alt",
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

  def joinOccurrencesWithClinical(occurrences: DataFrame, biospecimens: DataFrame, genomicFiles: DataFrame)(implicit spark: SparkSession): DataFrame = {

    occurrences
      .join(biospecimens, occurrences("biospecimen_id") === biospecimens("joined_sample_id"), "inner")
      .join(genomicFiles, occurrences("file_name") === genomicFiles("file_name"), "inner")
      .drop(occurrences("biospecimen_id")).drop(biospecimens("joined_sample_id")).drop(occurrences("file_name"))
  }

  def getBiospecimens(studyId: String, releaseId: String, biospecimenIdColumn: String)(implicit spark: SparkSession): DataFrame = {
    val biospecimen_id_col = col(biospecimenIdColumn).as("joined_sample_id")
    import spark.implicits._

    val b = loadClinicalTable(studyId, releaseId, "biospecimens")
      .select(biospecimen_id_col, $"biospecimen_id", $"participant_id", $"family_id").alias("b")
    val p = loadClinicalTable(studyId, releaseId, "participants").select("kf_id", "is_proband", "affected_status").alias("p")
    val all = b.join(p, b("participant_id") === p("kf_id")).select("b.*", "p.is_proband", "p.affected_status")

    broadcast(all)
  }

  def getRelations(studyId: String, releaseId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    broadcast(
      loadClinicalTable(studyId, releaseId, "family_relationships")
        .groupBy("participant2")
        .agg(
          map_from_entries(
            collect_list(
              struct($"participant1_to_participant2_relation" as "relation", $"participant1" as "participant_id")
            )
          ) as "relations"
        )
        .select($"participant2" as "participant_id", $"relations.Mother" as "mother_id", $"relations.Father" as "father_id")

    )

  }

  private def loadClinicalTable(studyId: String, releaseId: String, tableName: String)(implicit spark: SparkSession) = {
    import spark.implicits._
    spark
      .table(s"${tableName}_${releaseId.toLowerCase}")
      .where($"study_id" === studyId)
  }

  def getGenomicFiles(studyId: String, releaseId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    broadcast(
      spark
        .table(s"genomic_files_${releaseId.toLowerCase}")
        .where($"study_id" === studyId)
        .withColumn("acl", filterAcl($"acl"))
        .where(size($"acl") <= 1)
        .select($"acl"(0) as "acl", $"file_name")
        .select(when($"acl".isNull, "_NONE_").when($"acl" === "*", "_PUBLIC_").otherwise($"acl") as "dbgap_consent_code", $"file_name")
    )
  }

  def write(df: DataFrame, output: String, studyId: String, releaseId: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val tableOccurence = tableName("occurrences", studyId, releaseId)
    df
      .repartition($"has_alt", $"dbgap_consent_code", $"chromosome")
      .withColumn("bucket",
        functions
          .ntile(6)
          .over(
            Window.partitionBy("has_alt", "dbgap_consent_code", "chromosome")
              .orderBy("start")
          )
      )
      .repartition($"has_alt", $"dbgap_consent_code", $"chromosome", $"bucket")
      .sortWithinPartitions($"has_alt", $"dbgap_consent_code", $"chromosome", $"bucket", $"start")
      .write.mode(SaveMode.Overwrite)
      .partitionBy("study_id", "has_alt", "dbgap_consent_code", "chromosome")
      .format("parquet")
      .option("path", s"$output/occurrences/$tableOccurence")
      .saveAsTable(tableOccurence)
  }

}
