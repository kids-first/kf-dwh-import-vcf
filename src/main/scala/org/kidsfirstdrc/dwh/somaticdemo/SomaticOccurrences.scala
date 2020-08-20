package org.kidsfirstdrc.dwh.somaticdemo

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession, functions}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
import org.kidsfirstdrc.dwh.vcf.Occurrences.{filename, joinOccurrencesWithClinical}

object SomaticOccurrences {

  def run(studyId: String, releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {
    write(build(studyId, releaseId, input), output, studyId, releaseId)
  }

  def build(studyId: String, releaseId: String, input: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val inputDF = vcf(input)
    val occurrences = inputDF
      .withColumn("file_name", filename)
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
        lit(true) as "has_alt",
        is_multi_allelic,
        old_multi_allelic,
        $"file_name",
        lit(studyId) as "study_id",
        lit(releaseId) as "release_id"
      )
      .withColumn("hgvsg", lit(null).cast("string"))
      .withColumn("variant_class", variant_class)
      .drop("annotation")

    joinOccurrencesWithClinical(studyId, releaseId, occurrences)
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
