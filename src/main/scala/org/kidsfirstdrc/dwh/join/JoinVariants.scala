package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.join.JoinWrite.write
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.{calculated_duo_af, locusColumNames}
import org.kidsfirstdrc.dwh.utils.SparkUtils.firstAs
import org.kidsfirstdrc.dwh.vcf.Variants.TABLE_NAME

object JoinVariants {

  def join(studyIds: Seq[String], releaseId: String, output: String, mergeWithExisting: Boolean = true, database: String = "variant")(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val variants: DataFrame = studyIds.foldLeft(spark.emptyDataFrame) {
      (currentDF, studyId) =>
        val nextDf = spark.table(SparkUtils.tableName(TABLE_NAME, studyId, releaseId, database))
          .withColumn("studies", array($"study_id"))
          .withColumn("hmb_ac_by_study", map($"study_id", $"hmb_ac"))
          .withColumn("hmb_an_by_study", map($"study_id", $"hmb_an"))
          .withColumn("hmb_af_by_study", map($"study_id", $"hmb_af"))
          .withColumn("hmb_homozygotes_by_study", map($"study_id", $"hmb_homozygotes"))
          .withColumn("hmb_heterozygotes_by_study", map($"study_id", $"hmb_heterozygotes"))
          .withColumn("gru_ac_by_study", map($"study_id", $"gru_ac"))
          .withColumn("gru_an_by_study", map($"study_id", $"gru_an"))
          .withColumn("gru_af_by_study", map($"study_id", $"gru_af"))
          .withColumn("gru_homozygotes_by_study", map($"study_id", $"gru_homozygotes"))
          .withColumn("gru_heterozygotes_by_study", map($"study_id", $"gru_heterozygotes"))
        //.withColumn("freqs", struct($"an", $"ac", $"af", $"homozygotes", $"heterozygotes"))
        if (currentDF.isEmpty)
          nextDf
        else {
          currentDF
            .union(nextDf)
        }

    }

    val commonColumns = Seq($"chromosome", $"start", $"reference", $"alternate", $"end", $"name", $"hgvsg", $"variant_class",
      $"release_id", $"hmb_ac_by_study", $"hmb_an_by_study", $"hmb_af_by_study", $"hmb_homozygotes_by_study",
      $"hmb_heterozygotes_by_study", $"gru_ac_by_study", $"gru_an_by_study", $"gru_af_by_study", $"gru_homozygotes_by_study",
      $"gru_heterozygotes_by_study", $"studies", $"consent_codes", $"consent_codes_by_study"
    )

    val allColumns = commonColumns :+
      $"hmb_ac" :+ $"hmb_an" :+ $"hmb_homozygotes" :+ $"hmb_heterozygotes" :+
      $"gru_ac" :+ $"gru_an" :+ $"gru_homozygotes" :+ $"gru_heterozygotes" :+
      $"study_id"

    val merged = if (mergeWithExisting && spark.catalog.tableExists(TABLE_NAME)) {
      val existingColumns = commonColumns :+ explode($"studies").as("study_id")
      val existingVariants = spark.table(TABLE_NAME)
        .select(existingColumns: _*)
        .withColumn("hmb_ac", $"hmb_ac_by_study"($"study_id"))
        .withColumn("hmb_ac_by_study", map($"study_id", $"hmb_ac"))
        .withColumn("hmb_an", $"hmb_an_by_study"($"study_id"))
        .withColumn("hmb_an_by_study", map($"study_id", $"hmb_an"))
        .withColumn("hmb_af_by_study", map($"study_id", $"hmb_af_by_study"($"study_id")))
        .withColumn("hmb_homozygotes", $"hmb_homozygotes_by_study"($"study_id"))
        .withColumn("hmb_homozygotes_by_study", map($"study_id", $"hmb_homozygotes"))
        .withColumn("hmb_heterozygotes", $"hmb_heterozygotes_by_study"($"study_id"))
        .withColumn("hmb_heterozygotes_by_study", map($"study_id", $"hmb_heterozygotes"))
        .withColumn("gru_ac", $"gru_ac_by_study"($"study_id"))
        .withColumn("gru_ac_by_study", map($"study_id", $"gru_ac"))
        .withColumn("gru_an", $"gru_an_by_study"($"study_id"))
        .withColumn("gru_an_by_study", map($"study_id", $"gru_an"))
        .withColumn("gru_af_by_study", map($"study_id", $"gru_af_by_study"($"study_id")))
        .withColumn("gru_homozygotes", $"gru_homozygotes_by_study"($"study_id"))
        .withColumn("gru_homozygotes_by_study", map($"study_id", $"gru_homozygotes"))
        .withColumn("gru_heterozygotes", $"gru_heterozygotes_by_study"($"study_id"))
        .withColumn("gru_heterozygotes_by_study", map($"study_id", $"gru_heterozygotes"))
        .withColumn("consent_codes", $"consent_codes_by_study"($"study_id"))
        .withColumn("consent_codes_by_study", map($"study_id", $"consent_codes"))
        .where(not($"study_id".isin(studyIds: _*)))

      mergeVariants(
        releaseId, existingVariants
          .select(allColumns: _*)
          .union(variants.select(allColumns: _*))
      )
    } else {
      mergeVariants(releaseId, variants.select(allColumns: _*))
    }
    val joinedWithPop = joinWithPopulations(merged)
    val joinedWithClinvar = joinWithClinvar(joinedWithPop)
    val joinedWithDBSNP = joinWithDBSNP(joinedWithClinvar)

    write(releaseId, output, TABLE_NAME, joinedWithDBSNP, Some(60), database)

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

        sum("hmb_ac") as "hmb_ac",
        map_from_entries(collect_list(struct($"study_id", $"hmb_ac"))) as "hmb_ac_by_study",
        sum("hmb_an") as "hmb_an",
        map_from_entries(collect_list(struct($"study_id", $"hmb_an"))) as "hmb_an_by_study",
        map_from_entries(collect_list(struct($"study_id", calculated_duo_af("hmb")))) as "hmb_af_by_study",
        sum("hmb_homozygotes") as "hmb_homozygotes",
        map_from_entries(collect_list(struct($"study_id", $"hmb_homozygotes"))) as "hmb_homozygotes_by_study",
        sum("hmb_heterozygotes") as "hmb_heterozygotes",
        map_from_entries(collect_list(struct($"study_id", $"hmb_heterozygotes"))) as "hmb_heterozygotes_by_study",
        sum("gru_ac") as "gru_ac",
        map_from_entries(collect_list(struct($"study_id", $"gru_ac"))) as "gru_ac_by_study",
        sum("gru_an") as "gru_an",
        map_from_entries(collect_list(struct($"study_id", $"gru_an"))) as "gru_an_by_study",
        map_from_entries(collect_list(struct($"study_id", calculated_duo_af("gru")))) as "gru_af_by_study",
        sum("gru_homozygotes") as "gru_homozygotes",
        map_from_entries(collect_list(struct($"study_id", $"gru_homozygotes"))) as "gru_homozygotes_by_study",
        sum("gru_heterozygotes") as "gru_heterozygotes",
        map_from_entries(collect_list(struct($"study_id", $"gru_heterozygotes"))) as "gru_heterozygotes_by_study",
        collect_list($"study_id") as "studies",
        array_distinct(flatten(collect_list($"consent_codes"))) as "consent_codes",
        map_from_entries(collect_list(struct($"study_id", $"consent_codes"))) as "consent_codes_by_study",
        lit(releaseId) as "release_id"
      )
      .withColumn("hmb_af", calculated_duo_af("hmb"))
      .withColumn("gru_af", calculated_duo_af("gru"))
    t

  }

  def joinWithPopulations(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    //TODO remove .dropDuplicates(locusColumNames) when issue#2893 is fixed
    val genomes = spark.table("1000_genomes").dropDuplicates(locusColumNames)
    val topmed = spark.table("topmed_bravo").dropDuplicates(locusColumNames)
    val gnomad_genomes_2_1 = spark.table("gnomad_genomes_2_1_1_liftover_grch38").dropDuplicates(locusColumNames)
    val gnomad_exomes_2_1 = spark.table("gnomad_exomes_2_1_1_liftover_grch38").dropDuplicates(locusColumNames)
    val gnomad_genomes_3_0 = spark.table("gnomad_genomes_3_0").dropDuplicates(locusColumNames)

    variants
      .joinAndMerge(genomes, "1k_genomes")
      .joinAndMerge(topmed, "topmed")
      .joinAndMerge(gnomad_genomes_2_1, "gnomad_genomes_2_1")
      .joinAndMerge(gnomad_exomes_2_1, "gnomad_exomes_2_1")
      .joinAndMerge(gnomad_genomes_3_0, "gnomad_genomes_3_0")
  }

  def joinWithClinvar(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    //TODO remove .dropDuplicates(locusColumNames) when issue#2893 is fixed
    val clinvar = spark.table("clinvar").dropDuplicates(locusColumNames)
    variants
      .joinByLocus(clinvar)
      .select(variants("*"), clinvar("name") as "clinvar_id", clinvar("clin_sig") as "clin_sig")
  }

  def joinWithDBSNP(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    //TODO remove .dropDuplicates(locusColumNames) when issue#2893 is fixed
    val dbsnp = spark.table("dbsnp").dropDuplicates(locusColumNames)
    variants
      .joinByLocus(dbsnp)
      .select(variants("*"), dbsnp("name") as "dbsnp_id")
  }

}