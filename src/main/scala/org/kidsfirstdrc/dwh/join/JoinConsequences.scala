package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.catalyst.expressions.ArrayContains
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.utils.SparkUtils
import org.kidsfirstdrc.dwh.utils.SparkUtils.firstAs


object JoinConsequences {

  def join(studyIds: Seq[String], releaseId: String, output: String)(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val consequences: DataFrame = studyIds.foldLeft(spark.emptyDataFrame) {
      (currentDF, studyId) =>
        val nextDf = spark.table(SparkUtils.tableName("consequences", studyId, releaseId))
          .withColumn("study_ids", array($"study_id"))
        if (currentDF.isEmpty)
          nextDf
        else {
          currentDF
            .union(nextDf)
        }

    }

    val allColumns = Seq(
      $"chromosome",
      $"start",
      $"end",
      $"reference",
      $"alternate",
      $"consequence",
      $"ensembl_gene_id",
      $"strand",
      $"name",
      $"impact",
      $"hgvsg",
      $"variant_class",
      $"symbol",
      $"transcripts",
      $"study_ids",
      $"cds_position",
      $"amino_acids",
      $"protein_position"
    )
    val merged = if (spark.catalog.tableExists("consequences")) {

      val existingConsequences = spark.table("consequences")

      mergeConsequences(releaseId, existingConsequences.select(allColumns: _*)
        .union(consequences.select(allColumns: _*))
      )
    } else {
      mergeConsequences(releaseId, consequences.select(allColumns: _*))
    }
    merged.repartition($"chromosome")
      .sortWithinPartitions("start")
      .write.mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("parquet")
      .option("path", s"$output/consequences/consequences_$releaseId")
      .saveAsTable(s"consequences_$releaseId")

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
      $"ensembl_gene_id",
      $"strand",
      $"cds_position"
    )
      .agg(
        firstAs("name"),
        firstAs("impact"),
        firstAs("hgvsg"),
        firstAs("variant_class"),
        firstAs("symbol"),
        firstAs("protein_position"),
        firstAs("amino_acids"),
        firstAs("transcripts"),
        array_distinct(flatten(collect_list($"study_ids"))) as "study_ids"
      )
      .withColumn("aa_change", when($"amino_acids".isNotNull, concat($"amino_acids.reference", $"protein_position", $"amino_acids.variant")).otherwise(lit(null)))
      .withColumn("coding_dna_change", when($"cds_position".isNotNull, concat($"cds_position", $"reference", lit(">"), $"alternate")).otherwise(lit(null)))
      .withColumn("release_id", lit(releaseId))


  }


}
