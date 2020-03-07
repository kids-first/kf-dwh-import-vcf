package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object Consequences {
  def run(studyId: String, releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val inputDF = vcf(input)
    build(studyId, releaseId, inputDF)
    val consequences: DataFrame = build(studyId, releaseId, inputDF)

    val tableConsequences = tableName("consequences", studyId, releaseId)
    consequences
      .repartition($"chromosome")
      .sortWithinPartitions("start")
      .write.mode(SaveMode.Overwrite)
      .partitionBy("study_id", "release_id", "chromosome")
      .format("parquet")
      .option("path", s"$output/consequences/$tableConsequences")
      .saveAsTable(tableConsequences)

  }

  def build(studyId: String, releaseId: String, inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val consequencesDF = inputDF
      .select(
        chromosome,
        $"start",
        $"end",
        reference,
        alternate,
        name,
        annotations
      )
      .groupBy(locus: _*)
      .agg(
        first("annotations") as "annotations",
        first("name") as "name"
      )
      .withColumn("annotation", explode($"annotations"))
      .drop("annotations")
      .select($"*",
        consequences,
        impact,
        symbol,
        gene_id,
        transcript,
        strand,
        variant_class,
        hgvsg
      )
      .drop("split_annotation")
      .withColumn("consequence", explode($"consequences"))
      .drop("consequences")
      .groupBy(
        $"chromosome",
        $"start",
        $"end",
        $"reference",
        $"alternate",
        $"consequence",
        $"gene_id",
        $"strand"
      )
      .agg(
        firstAs("name"),
        firstAs("impact"),
        firstAs("hgvsg"),
        firstAs("variant_class"),
        firstAs("symbol"),
        collect_set($"transcript") as "transcripts"

      )
      .select($"*",
        lit(studyId) as "study_id",
        lit(releaseId) as "release_id"
      )

    consequencesDF
  }
}
