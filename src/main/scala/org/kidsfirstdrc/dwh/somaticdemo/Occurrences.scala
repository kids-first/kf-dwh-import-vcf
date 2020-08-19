package org.kidsfirstdrc.dwh.somaticdemo

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object Occurrences {

  def run(studyId: String, releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {
    write(build(studyId, releaseId, input), output, studyId, releaseId)
  }

  def build(studyId: String, releaseId: String, input: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val occurrences = vcf(input)
      .withColumn("genotype", $"genotypes"(1))
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        firstCsq,
        $"genotype.sampleId" as "biospecimen_id",
        $"genotype.alleleDepths" as "ad",
        $"genotype.depth" as "dp",
        $"genotype.conditionalQuality" as "gq",
        array(lit(0), lit(1)) as "calls",
        lit(1) as "has_alt",
        is_multi_allelic,
        old_multi_allelic,
        lit(studyId) as "study_id",
        lit(releaseId) as "release_id"
      )
      .withColumn("hgvsg", lit(null).cast("string"))
      .withColumn("variant_class", variant_class)
      .drop("annotation")

    val biospecimen_id_col = col("biospecimen_id").as("joined_sample_id")
    val biospecimens = broadcast(
      spark
        .table(s"biospecimens_${releaseId.toLowerCase}")
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

}
