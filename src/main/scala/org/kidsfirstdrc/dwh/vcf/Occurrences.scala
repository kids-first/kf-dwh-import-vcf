package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object Occurrences {

  def run(studyId: String, releaseId: String, input: String, output: String, biospecimenIdColumn: String)(implicit spark: SparkSession): Unit = {
    write(build(studyId, releaseId, input, biospecimenIdColumn), output, studyId, releaseId)
  }

  def build(studyId: String, releaseId: String, input: String, biospecimenIdColumn: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val occurrences = vcf(input)
      .withColumn("genotype", explode($"genotypes"))
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
        lit(studyId) as "study_id",
        lit(releaseId) as "release_id"
      )
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .drop("annotation")

    val biospecimen_id_col = col(biospecimenIdColumn).as("joined_sample_id")
    val biospecimens = broadcast(
      spark
        .table(s"biospecimens_$releaseId")
        .where($"study_id" === studyId)
        .select(biospecimen_id_col, $"biospecimen_id", $"participant_id", $"family_id", when($"dbgap_consent_code".isNotNull, $"dbgap_consent_code").otherwise("none") as "dbgap_consent_code")
    )

    occurrences
      .join(biospecimens, occurrences("biospecimen_id") === biospecimens("joined_sample_id"), "inner")
      .drop(occurrences("biospecimen_id")).drop(biospecimens("joined_sample_id"))
  }

  def write(df: DataFrame, output: String, studyId: String, releaseId: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val tableOccurence = tableName("occurrences", studyId, releaseId)
    df
      .repartition($"dbgap_consent_code", $"chromosome")
      .withColumn("bucket",
        functions
          .ntile(8)
          .over(
            Window.partitionBy("dbgap_consent_code", "chromosome")
              .orderBy("start")
          )
      )
      .repartition($"dbgap_consent_code", $"chromosome", $"bucket")
      .sortWithinPartitions($"dbgap_consent_code", $"chromosome", $"bucket", $"start")
      .write.mode(SaveMode.Overwrite)
      .partitionBy("study_id", "dbgap_consent_code", "chromosome")
      .format("parquet")
      .option("path", s"$output/occurrences/$tableOccurence")
      .saveAsTable(tableOccurence)
  }

  private def familyColumn(implicit spark: SparkSession) = {
    import spark.implicits._
    struct(
      $"qual" as "quality",
      //$"filters"(0) as "filter",
      $"INFO_loConfDeNovo" as "lo_conf_de_novo",
      $"INFO_NEGATIVE_TRAIN_SITE" as "negative_train_site",
      ac,
      an,
      $"INFO_culprit" as "culprit",
      $"INFO_SOR" as "sor",
      $"INFO_ReadPosRankSum" as "read_pos_rank_sum",
      $"INFO_InbreedingCoeff" as "inbreeding_coeff",
      $"INFO_PG" as "pg",
      $"INFO_AF"(0) as "af",
      $"INFO_FS" as "fs",
      $"INFO_DP" as "dp",
      $"INFO_POSITIVE_TRAIN_SITE" as "positive_train_site",
      $"INFO_VQSLOD" as "vqslod",
      $"INFO_ClippingRankSum" as "clipping_rank_sum",
      $"INFO_RAW_MQ" as "raw_mq",
      $"INFO_BaseQRankSum" as "base_qrank_sum",
      $"INFO_MLEAF"(0) as "mleaf",
      $"INFO_MLEAC"(0) as "mleac",
      $"INFO_MQ" as "mq",
      $"INFO_QD" as "qd",
      $"INFO_END" as "end",
      $"INFO_DB" as "db",
      $"INFO_MQRankSum" as "m_qrank_sum",
      $"INFO_hiConfDeNovo" as "hiconfdenovo",
      $"INFO_ExcessHet" as "excess_het"
    ) as "family"
  }
}
