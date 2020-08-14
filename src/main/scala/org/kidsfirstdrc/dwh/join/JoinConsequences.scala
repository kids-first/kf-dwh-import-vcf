package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.join.JoinWrite.write
import org.kidsfirstdrc.dwh.utils.SparkUtils
import org.kidsfirstdrc.dwh.utils.SparkUtils.firstAs


object JoinConsequences {

  val TABLE_NAME = "consequences"

  def join(studyIds: Seq[String], releaseId: String, output: String, mergeWithExisting: Boolean)(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val consequences: DataFrame = studyIds.foldLeft(spark.emptyDataFrame) {
      (currentDF, studyId) =>
        val nextDf = spark.table(SparkUtils.tableName("consequences", studyId, releaseId))
        if (currentDF.isEmpty)
          nextDf
        else {
          currentDF
            .union(nextDf)
        }

    }

    val commonColumns = Seq(
      $"chromosome",
      $"start",
      $"end",
      $"reference",
      $"alternate",
      $"consequence",
      $"ensembl_transcript_id",
      $"ensembl_regulatory_id",
      $"feature_type",
      $"name",
      $"impact",
      $"symbol",
      $"ensembl_gene_id",
      $"strand",
      $"biotype",
      $"variant_class",
      $"exon",
      $"intron",
      $"hgvsc",
      $"hgvsp",
      $"hgvsg",
      $"cds_position",
      $"cdna_position",
      $"protein_position",
      $"amino_acids",
      $"codons"
    )
    val allColumns = commonColumns :+ col("study_id")
    val merged = if (mergeWithExisting && spark.catalog.tableExists("consequences")) {

      val existingConsequences = spark.table("consequences")

      val existingColumns = commonColumns :+ $"study_ids"
      mergeConsequences(releaseId, existingConsequences.select(existingColumns: _*)
        .withColumn("study_id", explode($"study_ids"))
        .drop("study_ids")
        .where(not($"study_id".isin(studyIds: _*)))
        .union(consequences.select(allColumns: _*))
      )
    } else {
      mergeConsequences(releaseId, consequences.select(allColumns: _*))
    }
    val joinedWithScores = joinWithDBNSFP(merged)
    write(releaseId, output, TABLE_NAME, joinedWithScores, 1)

  }


  private def mergeConsequences(releaseId: String, consequences: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    consequences.groupBy(
      $"chromosome",
      $"start",
      $"end",
      $"reference",
      $"alternate",
      $"consequence",
      $"ensembl_transcript_id",
      $"ensembl_regulatory_id",
      $"feature_type"
    )
      .agg(
        firstAs("name"),
        firstAs("impact"),
        firstAs("symbol"),
        firstAs("ensembl_gene_id"),
        firstAs("strand"),
        firstAs("biotype"),
        firstAs("variant_class"),
        firstAs("exon"),
        firstAs("intron"),
        firstAs("hgvsc"),
        firstAs("hgvsp"),
        firstAs("hgvsg"),
        firstAs("cds_position"),
        firstAs("cdna_position"),
        firstAs("protein_position"),
        firstAs("amino_acids"),
        firstAs("codons"),
        collect_set($"study_id") as "study_ids"
      )
      .withColumn("aa_change", when($"amino_acids".isNotNull, concat($"amino_acids.reference", $"protein_position", $"amino_acids.variant")).otherwise(lit(null)))
      .withColumn("coding_dna_change", when($"cds_position".isNotNull, concat($"cds_position", $"reference", lit(">"), $"alternate")).otherwise(lit(null)))
      .withColumn("release_id", lit(releaseId))


  }


  def joinWithDBNSFP(c: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val s = spark.table("variant.dbnsfp_scores")
      .drop(
        "aaref",
        "symbol",
        "ensembl_gene_id",
        "ensembl_protein_id",
        "cds_strand")

    c.join(s,
      c("chromosome") === s("chromosome") &&
        c("start") === s("start") &&
        c("reference") === s("reference") &&
        c("alternate") === s("alternate") &&
        c("ensembl_transcript_id") === s("ensembl_transcript_id"),
      "left")
      .drop(s("chromosome"))
      .drop(s("start"))
      .drop(s("reference"))
      .drop(s("alternate"))
      .drop(s("ensembl_transcript_id"))


  }

}
