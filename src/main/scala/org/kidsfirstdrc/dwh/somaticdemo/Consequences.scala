package org.kidsfirstdrc.dwh.somaticdemo

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
        start,
        end,
        reference,
        alternate,
        name,
        csq
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
        ensembl_gene_id,
        ensembl_transcript_id,
        ensembl_regulatory_id,
        feature_type,
        strand,
        biotype,
        variant_class,
        exon,
        intron,
        hgvsc,
        hgvsp,
        lit(null).cast("string") as "hgvsg",
        cds_position,
        cdna_position,
        protein_position,
        amino_acids,
        codons,
        lit(studyId) as "study_id",
        lit(releaseId) as "release_id"
      )
      .drop("annotation")
      .withColumn("consequence", explode($"consequences"))
      .drop("consequences")

    consequencesDF
  }
}
