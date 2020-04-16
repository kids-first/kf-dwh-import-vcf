package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object Occurences {

  def run(studyId: String, releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {
    write(build(studyId, releaseId, input), output, studyId, releaseId)
  }

  def build(studyId: String, releaseId: String, input: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val occurences = vcf(input)
      .withColumn("genotype", explode($"genotypes"))
      .select(
        chromosome,
        $"start",
        $"end",
        reference,
        alternate,
        name,
        firstAnn,
        $"genotype.sampleId" as "biospecimen_id",
        $"genotype",
        array_contains($"genotype.calls", 1) as "has_alt",
        familyColumn,
        is_multi_allelic,
        old_multi_allelic,
        lit(studyId) as "study_id",
        lit(releaseId) as "release_id"
      )
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .drop("annotation")

    val biospecimens = broadcast(
      spark
        .table(tableName("biospecimens", studyId, releaseId))
        .select($"biospecimen_id", $"participant_id", $"family_id", when($"dbgap_consent_code".isNotNull, $"dbgap_consent_code").otherwise("none"))
    )

    occurences
      .join(biospecimens, occurences("biospecimen_id") === biospecimens("biospecimen_id"), "left_outer")
      .drop(biospecimens("biospecimen_id"))
  }

  def write(df: DataFrame, output: String, studyId: String, releaseId: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val tableOccurence = tableName("occurences", studyId, releaseId)
    df
      .repartition($"dbgap_consent_code", $"chromosome")
      .withColumn("bucket",
        functions
          .ntile(10)
          .over(
            Window.partitionBy("dbgap_consent_code", "chromosome")
              .orderBy("start")
          )
      )
      .repartition($"dbgap_consent_code", $"chromosome", $"bucket")
      .sortWithinPartitions($"dbgap_consent_code", $"chromosome", $"bucket", $"start")
      .write.mode(SaveMode.Overwrite)
      .partitionBy("study_id", "release_id", "dbgap_consent_code", "chromosome")
      .format("parquet")
      .option("path", s"$output/occurences/$tableOccurence")
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
