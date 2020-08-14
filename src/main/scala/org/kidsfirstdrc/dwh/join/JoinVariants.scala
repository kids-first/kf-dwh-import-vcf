package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.join.JoinWrite.write
import org.kidsfirstdrc.dwh.utils.SparkUtils
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.calculated_af
import org.kidsfirstdrc.dwh.utils.SparkUtils.firstAs
import org.kidsfirstdrc.dwh.vcf.Variants.TABLE_NAME


object JoinVariants {

  def join(studyIds: Seq[String], releaseId: String, output: String, mergeWithExisting: Boolean = true)(implicit spark: SparkSession): Unit = {

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

    val commonColumns = Seq($"chromosome", $"start", $"reference", $"alternate", $"end", $"name", $"hgvsg", $"variant_class", $"release_id")
    val allColumns = commonColumns :+ $"freqs" :+ $"study_id"
    val merged = if (mergeWithExisting && spark.catalog.tableExists(TABLE_NAME)) {
      val existingColumns = commonColumns :+ explode($"by_study")
      val existingVariants = spark.table(TABLE_NAME)
        .select(existingColumns: _*)
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
    val joinedWithDBSNP = joinWithDBSNP(joinedWithClinvar)

    write(releaseId, output, TABLE_NAME, joinedWithDBSNP, 5)

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
    val gnomad_genomes_2_1 = spark.table("gnomad_genomes_2_1_1_liftover_grch38")
    val gnomad_exomes_2_1 = spark.table("gnomad_exomes_2_1_1_liftover_grch38")
    val gnomad_genomes_3_0 = spark.table("gnomad_genomes_3_0")

    val join1k = joinAndMerge(variants, genomes, "1k_genomes")
    val joinTopmed = joinAndMerge(join1k, topmed, "topmed")
    val join_gnomad_genomes_2_1 = joinAndMerge(joinTopmed, gnomad_genomes_2_1, "gnomad_genomes_2_1")
    val join_gnomad_exomes_2_1 = joinAndMerge(join_gnomad_genomes_2_1, gnomad_exomes_2_1, "gnomad_exomes_2_1")
    val join_gnomad_genomes_3_0 = joinAndMerge(join_gnomad_exomes_2_1, gnomad_genomes_3_0, "gnomad_genomes_3_0")
    join_gnomad_genomes_3_0
  }

  def joinAndMerge(df1: DataFrame, df2: DataFrame, outputColumnName: String): DataFrame = {
    joinByLocus(df1, df2)
      .select(df1("*"), when(df2("chromosome").isNull, lit(null)).otherwise(struct(df2.drop("chromosome", "start", "end", "name", "reference", "alternate")("*"))) as outputColumnName)
  }


  private def joinByLocus(df1: DataFrame, df2: DataFrame) = {
    df1.join(df2, df1("chromosome") === df2("chromosome") && df1("start") === df2("start") && df1("reference") === df2("reference") && df1("alternate") === df2("alternate"), "left")
  }

  def joinWithClinvar(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val clinvar = spark.table("clinvar")
    joinByLocus(variants, clinvar)
      .select(variants("*"), clinvar("name") as "clinvar_id", clinvar("clin_sig") as "clin_sig")
  }

  def joinWithDBSNP(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val dbsnp = spark.table("dbsnp")
    joinByLocus(variants, dbsnp)
      .select(variants("*"), dbsnp("name") as "dbsnp_id")
  }

}
