package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object Variants {
  val TABLE_NAME = "variants"

  def run(studyId: String, releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val inputDF = spark.table(tableName("occurrences", studyId, releaseId))
    val variants: DataFrame = build(studyId, releaseId, inputDF)
    val tableAnnotations = tableName(TABLE_NAME, studyId, releaseId)
    variants
      .repartition($"chromosome")
      .sortWithinPartitions("start")
      .write.mode(SaveMode.Overwrite)
      .partitionBy("study_id", "release_id", "chromosome")
      .format("parquet")
      .option("path", s"$output/$TABLE_NAME/$tableAnnotations")
      .saveAsTable(tableAnnotations)

  }

  def build(studyId: String, releaseId: String, inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val variants = inputDF
      .select(
        $"chromosome",
        $"start",
        $"end",
        $"reference",
        $"alternate",
        $"name",
        $"zygosity",
        calculated_ac,
        calculate_an,
        homozygotes,
        heterozygotes,
        $"is_gru",
        $"is_hmb",
        $"variant_class",
        $"hgvsg"
      )
      .groupBy(locus: _*)
      .agg(
        firstAs("name"),
        firstAs("hgvsg") +:
        firstAs("variant_class") +:
        (freqByDuoCode("hmb") ++ freqByDuoCode("gru")) :_*
      )
      .withColumn("hmb_af", calculated_duo_af("hmb"))
      .withColumn("gru_af", calculated_duo_af("gru"))
      .withColumn("study_id", lit(studyId))
      .withColumn("release_id", lit(releaseId))
    variants
  }


  def freqByDuoCode(duo: String): Seq[Column] = {
    Seq(
      sum(when(col(s"is_$duo"), col("ac")).otherwise(0)) as s"${duo}_ac",
      sum(when(col(s"is_$duo"), col("an")).otherwise(0)) as s"${duo}_an",
      sum(when(col(s"is_$duo"), col("homozygotes")).otherwise(0)) as s"${duo}_homozygotes",
      sum(when(col(s"is_$duo"), col("heterozygotes")).otherwise(0)) as s"${duo}_heterozygotes"
    )
  }
}
