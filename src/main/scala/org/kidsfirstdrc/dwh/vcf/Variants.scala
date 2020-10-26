package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.functions.{lit, sum}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
import org.kidsfirstdrc.dwh.utils.ClinicalUtils.getGenomicFiles

object Variants {
  val TABLE_NAME = "variants"

  def run(studyId: String, releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val inputDF = visibleVcf(input, studyId, releaseId)
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
    val annotations = inputDF
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        ac,
        an,
        name,
        firstAnn,
        homozygotes,
        heterozygotes
      )
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .drop("annotation")
      .groupBy(locus: _*)
      .agg(
        sum("ac") as "ac",
        sum("an") as "an",
        sum("homozygotes") as "homozygotes",
        sum("heterozygotes") as "heterozygotes",
        firstAs("name"),
        firstAs("hgvsg"),
        firstAs("variant_class")
      )
      .select($"*", calculated_af, lit(studyId) as "study_id", lit(releaseId) as "release_id")
    annotations
  }
}
