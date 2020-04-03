package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.utils.SparkUtils
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.calculated_af
import org.kidsfirstdrc.dwh.utils.SparkUtils.firstAs
import org.kidsfirstdrc.dwh.vcf.Variants.TABLE_NAME


object JoinVariants {
    
  def join(studyIds: Seq[String], releaseId: String, output: String)(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val variants: DataFrame = studyIds.foldLeft(spark.emptyDataFrame) {
      (currentDF, studyId) =>
        val nextDf = spark.table(SparkUtils.tableName(TABLE_NAME, studyId, releaseId))
          .withColumn("freqs", struct($"an", $"ac", $"af", $"homozygotes", $"heterozygotes"))
        if (currentDF.isEmpty)
          nextDf
        else {
          currentDF
            .union(nextDf)
        }

    }

    val allColumns = Seq($"chromosome", $"start", $"reference", $"alternate", $"end", $"name", $"hgvsg", $"variant_class", $"release_id", $"freqs", $"study_id")
    val merged = if (spark.catalog.tableExists(TABLE_NAME)) {

      val existingVariants = spark.table(TABLE_NAME)
        .select(
          $"chromosome", $"start", $"reference", $"alternate", $"end", $"name", $"hgvsg", $"variant_class", $"release_id",
          explode($"by_study")
        )
        .withColumnRenamed("key", "study_id")
        .withColumnRenamed("value", "freqs")
        .where(not($"study_id".isin(studyIds: _*)))
      mergeVariants(releaseId, existingVariants
        .select(allColumns: _*)
        .union(variants.select(allColumns: _*))
      )
    } else {
      mergeVariants(releaseId, variants.select(allColumns: _*))
    }
    val joinedWithPop = joinWithPopulations(merged)
    val joinedWithClinvar = joinWithClinvar(joinedWithPop)
    joinedWithClinvar
      .repartition($"chromosome")
      .sortWithinPartitions("start")
      .write.mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("parquet")
      .option("path", s"$output/$TABLE_NAME/${TABLE_NAME}_$releaseId")
      .saveAsTable(s"${TABLE_NAME}_$releaseId")

  }


  private def mergeVariants(releaseId: String, variants: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val t = variants
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

  def joinWithPopulations(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val genomes = spark.table("1000_genomes")
    val topmed = spark.table("topmed_bravo")
    val gnomad = spark.table("gnomad_genomes_2_1_1_liftover_grch38")

    val join1k = variants
      .join(genomes, variants("chromosome") === genomes("chromosome") && variants("start") === genomes("start") && variants("reference") === genomes("reference") && variants("alternate") === genomes("alternate"), "left")
      .select(variants("*"), when(genomes("chromosome").isNull, lit(null)).otherwise(struct(genomes.drop("chromosome", "start", "end", "name", "reference", "alternate")("*"))) as "1k_genomes")

    val joinTopmed = join1k.join(topmed, join1k("chromosome") === topmed("chromosome") && join1k("start") === topmed("start") && join1k("reference") === topmed("reference") && join1k("alternate") === topmed("alternate"), "left")
      .select(join1k("*"), when(topmed("chromosome").isNull, lit(null)).otherwise(struct(topmed.drop("chromosome", "start", "end", "name", "reference", "alternate")("*"))) as "topmed")

    joinTopmed.join(gnomad, joinTopmed("chromosome") === gnomad("chromosome") && joinTopmed("start") === gnomad("start") && joinTopmed("reference") === gnomad("reference") && joinTopmed("alternate") === gnomad("alternate"), "left")
      .select(joinTopmed("*"), when(gnomad("chromosome").isNull, lit(null)).otherwise(struct(gnomad.drop("chromosome", "start", "end", "name", "reference", "alternate")("*"))) as "gnomad_2_1")

  }

  def joinWithClinvar(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val clinvar = spark.table("clinvar")
    variants.join(clinvar, variants("chromosome") === clinvar("chromosome") && variants("start") === clinvar("start") && variants("reference") === clinvar("reference") && variants("alternate") === clinvar("alternate"), "left")
      .select(variants("*"), clinvar("name") as "clinvar_id", clinvar("clin_sig") as "clin_sig")
  }

}
