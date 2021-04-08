package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.Clinical
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.{calculated_duo_af, locusColumNames}
import org.kidsfirstdrc.dwh.utils.SparkUtils.firstAs

object JoinVariants {

  def join(studyIds: Seq[String], releaseId: String, output: String, mergeWithExisting: Boolean, database: String)(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val variants: DataFrame = studyIds.foldLeft(spark.emptyDataFrame) {
      (currentDF, studyId) =>
        val nextDf = spark.table(SparkUtils.tableName(Clinical.variants.name, studyId, releaseId, database))
          .withColumn("studies", array($"study_id"))
          .withRenamedFrequencies("upper_bound_kf")
          .withRenamedFrequencies("lower_bound_kf")
          .withColumnByStudy("upper_bound_kf")
          .withColumnByStudy("lower_bound_kf")
        if (currentDF.isEmpty)
          nextDf
        else {
          currentDF
            .union(nextDf)
        }

    }

    val commonColumns = Seq($"chromosome", $"start", $"reference", $"alternate", $"end", $"name", $"hgvsg", $"variant_class",
      $"release_id",
      $"lower_bound_kf_ac_by_study",
      $"lower_bound_kf_an_by_study",
      $"lower_bound_kf_af_by_study",
      $"lower_bound_kf_homozygotes_by_study",
      $"lower_bound_kf_heterozygotes_by_study",
      $"upper_bound_kf_ac_by_study",
      $"upper_bound_kf_an_by_study",
      $"upper_bound_kf_af_by_study",
      $"upper_bound_kf_homozygotes_by_study",
      $"upper_bound_kf_heterozygotes_by_study",
      $"lower_bound_kf_ac",
      $"lower_bound_kf_an",
      $"lower_bound_kf_af",
      $"lower_bound_kf_homozygotes",
      $"lower_bound_kf_heterozygotes",
      $"upper_bound_kf_ac",
      $"upper_bound_kf_an",
      $"upper_bound_kf_af",
      $"upper_bound_kf_homozygotes",
      $"upper_bound_kf_heterozygotes",

      $"studies", $"consent_codes", $"consent_codes_by_study"
    )

    val allColumns = commonColumns :+
      $"study_id"

    val merged = if (mergeWithExisting && spark.catalog.tableExists(s"${database}.${Clinical.variants.name}")) {
      val existingColumns = commonColumns :+ explode($"studies").as("study_id")
      val existingVariants = spark.table(s"${database}.${Clinical.variants.name}")
        .withRenamedFrequencies("upper_bound_kf")
        .withRenamedFrequencies("lower_bound_kf")
        .select(existingColumns: _*)
        .withColumnByStudyAfterExplode("lower_bound_kf")
        .withColumnByStudyAfterExplode("upper_bound_kf")
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

    JoinWrite.write(releaseId, output, Clinical.variants.name, joinedWithDBSNP, Some(60), database)

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

        sum("lower_bound_kf_ac") as "lower_bound_kf_ac",
        map_from_entries(collect_list(struct($"study_id", $"lower_bound_kf_ac"))) as "lower_bound_kf_ac_by_study",
        sum("lower_bound_kf_an") as "lower_bound_kf_an",
        map_from_entries(collect_list(struct($"study_id", $"lower_bound_kf_an"))) as "lower_bound_kf_an_by_study",
        map_from_entries(collect_list(struct($"study_id", calculated_duo_af("lower_bound_kf")))) as "lower_bound_kf_af_by_study",
        sum("lower_bound_kf_homozygotes") as "lower_bound_kf_homozygotes",
        map_from_entries(collect_list(struct($"study_id", $"lower_bound_kf_homozygotes"))) as "lower_bound_kf_homozygotes_by_study",
        sum("lower_bound_kf_heterozygotes") as "lower_bound_kf_heterozygotes",
        map_from_entries(collect_list(struct($"study_id", $"lower_bound_kf_heterozygotes"))) as "lower_bound_kf_heterozygotes_by_study",

        sum("upper_bound_kf_ac") as "upper_bound_kf_ac",
        map_from_entries(collect_list(struct($"study_id", $"upper_bound_kf_ac"))) as "upper_bound_kf_ac_by_study",
        sum("upper_bound_kf_an") as "upper_bound_kf_an",
        map_from_entries(collect_list(struct($"study_id", $"upper_bound_kf_an"))) as "upper_bound_kf_an_by_study",
        map_from_entries(collect_list(struct($"study_id", calculated_duo_af("upper_bound_kf")))) as "upper_bound_kf_af_by_study",
        sum("upper_bound_kf_homozygotes") as "upper_bound_kf_homozygotes",
        map_from_entries(collect_list(struct($"study_id", $"upper_bound_kf_homozygotes"))) as "upper_bound_kf_homozygotes_by_study",
        sum("upper_bound_kf_heterozygotes") as "upper_bound_kf_heterozygotes",
        map_from_entries(collect_list(struct($"study_id", $"upper_bound_kf_heterozygotes"))) as "upper_bound_kf_heterozygotes_by_study",


        collect_list($"study_id") as "studies",
        array_distinct(flatten(collect_list($"consent_codes"))) as "consent_codes",
        map_from_entries(collect_list(struct($"study_id", $"consent_codes"))) as "consent_codes_by_study",
        lit(releaseId) as "release_id"
      )
      .withColumn("upper_bound_kf_af", calculated_duo_af("upper_bound_kf"))
      .withColumn("lower_bound_kf_af", calculated_duo_af("lower_bound_kf"))
      .withColumn("frequencies", struct(
        struct(
          col("upper_bound_kf_an") as "an",
          col("upper_bound_kf_ac") as "ac",
          col("upper_bound_kf_af") as "af",
          col("upper_bound_kf_homozygotes") as "homozygotes",
          col("upper_bound_kf_heterozygotes") as "heterozygotes") as "upper_bound_kf",
        struct(
          col("lower_bound_kf_an") as "an",
          col("lower_bound_kf_ac") as "ac",
          col("lower_bound_kf_af") as "af",
          col("lower_bound_kf_homozygotes") as "homozygotes",
          col("lower_bound_kf_heterozygotes") as "heterozygotes") as "lower_bound_kf"
      ))
      .drop("lower_bound_kf_an", "lower_bound_kf_ac", "lower_bound_kf_af", "lower_bound_kf_homozygotes", "lower_bound_kf_heterozygotes")
      .drop("upper_bound_kf_an", "upper_bound_kf_ac", "upper_bound_kf_af", "upper_bound_kf_homozygotes", "upper_bound_kf_heterozygotes")

    t

  }

  def joinWithPopulations(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    //TODO remove .dropDuplicates(locusColumNames) when issue#2893 is fixed
    import spark.implicits._
    val genomes = spark.table("variant.1000_genomes").dropDuplicates(locusColumNames)
      .selectLocus($"ac", $"an", $"af")
    val topmed = spark.table("variant.topmed_bravo").dropDuplicates(locusColumNames)
      .selectLocus($"ac", $"an", $"af", $"homozygotes", $"heterozygotes")
    val gnomad_genomes_2_1 = spark.table("variant.gnomad_genomes_2_1_1_liftover_grch38").dropDuplicates(locusColumNames)
      .selectLocus($"ac", $"an", $"af", $"hom")
    val gnomad_exomes_2_1 = spark.table("variant.gnomad_exomes_2_1_1_liftover_grch38").dropDuplicates(locusColumNames)
      .selectLocus($"ac", $"an", $"af", $"hom")
    val gnomad_genomes_3_0 = spark.table("variant.gnomad_genomes_3_0").dropDuplicates(locusColumNames)
      .selectLocus($"ac", $"an", $"af", $"hom")

    variants
      .joinAndMerge(genomes, "1k_genomes", "left")
      .joinAndMerge(topmed, "topmed", "left")
      .joinAndMerge(gnomad_genomes_2_1, "gnomad_genomes_2_1", "left")
      .joinAndMerge(gnomad_exomes_2_1, "gnomad_exomes_2_1", "left")
      .joinAndMerge(gnomad_genomes_3_0, "gnomad_genomes_3_0", "left")
  }

  def joinWithClinvar(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    //TODO remove .dropDuplicates(locusColumNames) when issue#2893 is fixed
    val clinvar = spark.table("variant.clinvar").dropDuplicates(locusColumNames)
    variants
      .joinByLocus(clinvar, "left")
      .select(variants("*"), clinvar("name") as "clinvar_id", clinvar("clin_sig") as "clin_sig")
  }

  def joinWithDBSNP(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    //TODO remove .dropDuplicates(locusColumNames) when issue#2893 is fixed
    val dbsnp = spark.table("variant.dbsnp").dropDuplicates(locusColumNames)
    variants
      .joinByLocus(dbsnp, "left")
      .select(variants("*"), dbsnp("name") as "dbsnp_id")
  }

  implicit class DataFrameOperations(df: DataFrame) {
    def withRenamedFrequencies(prefix: String): DataFrame = {
      df.withColumn(s"${prefix}_ac", col(s"frequencies.${prefix}.ac"))
        .withColumn(s"${prefix}_an", col(s"frequencies.${prefix}.an"))
        .withColumn(s"${prefix}_af", col(s"frequencies.${prefix}.af"))
        .withColumn(s"${prefix}_homozygotes", col(s"frequencies.${prefix}.homozygotes"))
        .withColumn(s"${prefix}_heterozygotes", col(s"frequencies.${prefix}.heterozygotes"))
    }

    def withColumnByStudy(prefix: String): DataFrame = {
      df.withColumn(s"${prefix}_ac_by_study", map(col("study_id"), col(s"frequencies.${prefix}.ac")))
        .withColumn(s"${prefix}_an_by_study", map(col("study_id"), col(s"frequencies.${prefix}.an")))
        .withColumn(s"${prefix}_af_by_study", map(col("study_id"), col(s"frequencies.${prefix}.af")))
        .withColumn(s"${prefix}_homozygotes_by_study", map(col("study_id"), col(s"frequencies.${prefix}.homozygotes")))
        .withColumn(s"${prefix}_heterozygotes_by_study", map(col("study_id"), col(s"frequencies.${prefix}.heterozygotes")))
    }

    def withColumnByStudyAfterExplode(prefix: String) = {
      df.withColumn(s"${prefix}_ac", col(s"${prefix}_ac_by_study")(col("study_id")))
        .withColumn(s"${prefix}_ac_by_study", map(col("study_id"), col(s"${prefix}_ac")))
        .withColumn(s"${prefix}_an", col(s"${prefix}_an_by_study")(col("study_id")))
        .withColumn(s"${prefix}_an_by_study", map(col("study_id"), col(s"${prefix}_an")))
        .withColumn(s"${prefix}_af_by_study", map(col("study_id"), col(s"${prefix}_af_by_study")(col("study_id"))))
        .withColumn(s"${prefix}_homozygotes", col(s"${prefix}_homozygotes_by_study")(col("study_id")))
        .withColumn(s"${prefix}_homozygotes_by_study", map(col("study_id"), col(s"${prefix}_homozygotes")))
        .withColumn(s"${prefix}_heterozygotes", col(s"${prefix}_heterozygotes_by_study")(col("study_id")))
        .withColumn(s"${prefix}_heterozygotes_by_study", map(col("study_id"), col(s"${prefix}_heterozygotes")))
    }
  }

}