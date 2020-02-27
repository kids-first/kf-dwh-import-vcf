package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.utils.SparkUtils
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.calculated_af
import org.kidsfirstdrc.dwh.utils.SparkUtils.firstAs


object JoinAnnotations {
  def join(studyIds: Seq[String], releaseId: String, output: String)(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val annotations: DataFrame = studyIds.foldLeft(spark.emptyDataFrame) {
      (currentDF, studyId) =>
        val nextDf = spark.table(SparkUtils.tableName("annotations", studyId, releaseId))
          .withColumn("freqs", struct($"an", $"ac", $"af", $"homozygotes", $"heterozygotes"))
        if (currentDF.isEmpty)
          nextDf
        else {
          currentDF
            .union(nextDf)
        }

    }

    val allColumns = Seq($"chromosome", $"start", $"reference", $"alternate", $"end", $"name", $"hgvsg", $"variant_class", $"release_id", $"freqs", $"study_id")
    val merged = if (spark.catalog.tableExists("annotations")) {

      val existingAnnotations = spark.table("annotations")
        .select(
          $"chromosome", $"start", $"reference", $"alternate", $"end", $"name", $"hgvsg", $"variant_class", $"release_id",
          explode($"by_study")
        )
        .withColumnRenamed("key", "study_id")
        .withColumnRenamed("value", "freqs")
        .where(not($"study_id".isin(studyIds: _*)))
      mergeAnnotations(releaseId, existingAnnotations
        .select(allColumns: _*)
        .union(annotations.select(allColumns: _*))
      )
    } else {
      mergeAnnotations(releaseId, annotations.select(allColumns: _*))
    }
    val joinedWithPop = joinWithPopulations(merged)
    joinedWithPop.repartition($"chromosome")
      .sortWithinPartitions("start")
      .write.mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("parquet")
      .option("path", s"$output/annotations/annotations_$releaseId")
      .saveAsTable(s"annotations_$releaseId")

  }


  private def mergeAnnotations(releaseId: String, annotations: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val t = annotations
      .groupBy($"chromosome", $"start", $"reference", $"alternate")
      .agg(
        firstAs("name"),
        firstAs("end"),
        firstAs("hgvsg"),
        firstAs("variant_class"),
        map_from_entries(collect_list(struct($"study_id", $"freqs"))) as "by_study"
      )

    t.withColumn("freqs", map_values($"by_study"))
      .withColumn("ac", expr("aggregate(freqs, 0L, (acc, x) -> acc + x.ac)"))
      .withColumn("an", expr("aggregate(freqs, 0L, (acc, x) -> acc + x.an)"))
      .withColumn("homozygotes", expr("aggregate(freqs, 0L, (acc, x) -> acc + x.homozygotes)"))
      .withColumn("heterozygotes", expr("aggregate(freqs, 0L, (acc, x) -> acc + x.heterozygotes)"))
      .drop("freqs")
      .select($"*", calculated_af, lit(releaseId) as "release_id")

  }

  def joinWithPopulations(annotations: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val genomes = spark.table("1000_genomes")
    val topmed = spark.table("topmed_bravo")
    val gnomad = spark.table("gnomad_genomes_2_1_1_liftover_grch38")

    val join1k = annotations
      .join(genomes, annotations("chromosome") === genomes("chromosome") && annotations("start") === genomes("start") && annotations("reference") === genomes("reference") && annotations("alternate") === genomes("alternate"), "left")
      .select(annotations("*"), when(genomes("chromosome").isNull, lit(null)).otherwise(struct(genomes.drop("chromosome", "start", "end", "name", "reference", "alternate")("*"))) as "1k_genomes")

    val joinTopmed = join1k.join(topmed, join1k("chromosome") === topmed("chromosome") && join1k("start") === topmed("start") && join1k("reference") === topmed("reference") && join1k("alternate") === topmed("alternate"), "left")
      .select(join1k("*"), when(topmed("chromosome").isNull, lit(null)).otherwise(struct(topmed.drop("chromosome", "start", "end", "name", "reference", "alternate")("*"))) as "topmed")

    joinTopmed.join(gnomad, joinTopmed("chromosome") === gnomad("chromosome") && joinTopmed("start") === gnomad("start") && joinTopmed("reference") === gnomad("reference") && joinTopmed("alternate") === gnomad("alternate"), "left")
      .select(joinTopmed("*"), when(gnomad("chromosome").isNull, lit(null)).otherwise(struct(gnomad.drop("chromosome", "start", "end", "name", "reference", "alternate")("*"))) as "gnomad_2_1")

  }

}
