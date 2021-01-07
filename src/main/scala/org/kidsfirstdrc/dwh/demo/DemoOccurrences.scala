package org.kidsfirstdrc.dwh.demo

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.vcf.Occurrences

object DemoOccurrences {

  def run(studyId: String, releaseId: String, input: String, output: String, isPostCGPOnly: Boolean)(implicit spark: SparkSession): Unit = {
    write(build(studyId, releaseId, input, isPostCGPOnly), output, studyId, releaseId)
  }

  def build(studyId: String, releaseId: String, input: String, isPostCGPOnly: Boolean)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val occurrences = Occurrences.selectOccurrences(studyId, releaseId, input, isPostCGPOnly)
      .withColumn("participant_id", $"biospecimen_id")
    val relations = broadcast(spark.read.option("sep", "\t")
      .option("header", "true")
      .csv("s3a://kf-variant-parquet-prd/raw/1000Genomes/20130606_g1k.ped")
      .select(
        when($"Family ID" === 0, lit(null).cast("string")).otherwise($"Family ID") as "family_id",
        $"Individual ID" as "participant_id",
        when($"Maternal ID" === 0, lit(null).cast("string")).otherwise($"Maternal ID") as "mother_id",
        when($"Paternal ID" === 0, lit(null).cast("string")).otherwise($"Paternal ID") as "father_id",
        $"Population" as "ethnicity"
      ))

    Occurrences.joinOccurrencesWithInheritence(occurrences, relations)


  }

  def write(df: DataFrame, output: String, studyId: String, releaseId: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val tableOccurence = tableName("occurrences", studyId, releaseId)
    df
      .repartition($"has_alt", $"chromosome")
      .withColumn("bucket",
        functions
          .ntile(4)
          .over(
            Window.partitionBy("has_alt", "chromosome")
              .orderBy("start")
          )
      )
      .repartition($"has_alt", $"chromosome", $"bucket")
      .sortWithinPartitions($"has_alt", $"chromosome", $"bucket", $"start")
      .write.mode(SaveMode.Overwrite)
      .partitionBy("study_id", "has_alt", "chromosome")
      .format("parquet")
      .option("path", s"$output/occurrences/$tableOccurence")
      .saveAsTable(tableOccurence)
  }

}
