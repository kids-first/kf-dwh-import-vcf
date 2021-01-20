package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object Consequences {
  def run(studyId: String, releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {
    val inputDF = visibleVcf(allFilesPath(input), studyId, releaseId)
      .select(chromosome, start, end, reference, alternate, name, annotations)

    val consequences = build(studyId, releaseId, inputDF)

    write(consequences, studyId, releaseId, output)
  }

  def build(studyId: String, releaseId: String, inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val consequencesDF = inputDF
      .groupBy(locus: _*)
      .agg(
        first("annotations") as "annotations",
        first("name") as "name",
        first("end") as "end"
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
        hgvsg,
        cds_position,
        cdna_position,
        protein_position,
        amino_acids,
        codons,
        canonical,
        lit(studyId) as "study_id",
        lit(releaseId) as "release_id"
      )
      .drop("annotation")

    consequencesDF
  }

  def write(inputDF: DataFrame, studyId: String, releaseId: String, output: String): Unit = {
    val tableConsequences = tableName("consequences", studyId, releaseId)
    val salt = (rand * 3).cast(IntegerType)  //3 files per chr, tried with 1 file per chr but got an OOM when writing parquet files
    inputDF
      .repartition(69, col("chromosome"), salt)
      .write.mode(SaveMode.Overwrite)
      .partitionBy("study_id", "release_id", "chromosome")
      .format("parquet")
      .option("path", s"$output/consequences/$tableConsequences")
      .saveAsTable(tableConsequences)
  }
}
