package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.{DataSource, ETL}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Raw}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

class Variants(studyId: String, releaseId: String, schema: String)(implicit conf: Configuration)
  extends ETL(Clinical.variants){

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    val participantsPath = Raw.all_participants.location
    val occurrencesPath = s"${Clinical.occurrences.rootPath}/occurrences/${tableName(Clinical.occurrences.name, studyId, releaseId)}"

    println(s"participantsPath: ${participantsPath}")
    println(s"occurrencesPath: ${occurrencesPath}")
    Map(
      Raw.all_participants ->
        spark.read.json(participantsPath),
      Clinical.occurrences ->
        spark.read.parquet(occurrencesPath)
    )
  }

  override def run()(implicit spark: SparkSession): DataFrame = {
    val inputDF = extract()(spark)
    val variants: DataFrame = transform(inputDF)
    load(variants)
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val participants = data(Raw.all_participants).select($"id" as "participant_id")

    val occurrences: DataFrame = schema match {
      case "portal" => data(Clinical.occurrences).join(broadcast(participants), Seq("participant_id"), "inner")
      case _ => data(Clinical.occurrences)
    }

    val participantTotalCount = occurrences.select("participant_id").distinct().repartition(200).count()

    occurrences
      .select(
        $"chromosome",
        $"start",
        $"end",
        $"reference",
        $"alternate",
        $"name",
        $"zygosity",
        calculated_ac,
        calculate_an_lower_bound_kf,
        homozygotes,
        heterozygotes,
        $"is_gru",
        $"is_hmb",
        $"variant_class",
        $"hgvsg",
        $"dbgap_consent_code"
      )
      .groupBy(locus: _*)
      .agg(
        firstAs("name"),
        firstAs("hgvsg"),
        firstAs("end"),
        firstAs("variant_class"),
        collect_set($"dbgap_consent_code").as("consent_codes"),
        sum(col("ac")) as "ac",
        sum(col("an_lower_bound_kf")) as "an_lower_bound_kf",
        sum(col("homozygotes")) as "homozygotes",
        sum(col("heterozygotes")) as "heterozygotes"
      )
      .withColumn("an_upper_bound_kf", calculate_an_upper_bound_kf(participantTotalCount))
      .withColumn("frequencies", struct(
        struct(
          col("ac"),
          col("an_upper_bound_kf") as "an",
          calculated_af_from_an(col("an_upper_bound_kf")) as "af",
          col("homozygotes"),
          col("heterozygotes")
      ) as "upper_bound_kf",
        struct(
          col("ac"),
          col("an_lower_bound_kf") as "an",
          calculated_af_from_an(col("an_lower_bound_kf")) as "af",
          col("homozygotes"),
          col("heterozygotes")
      ) as "lower_bound_kf"))
      .drop("an", "ac", "af", "heterozygotes", "homozygotes", "an_upper_bound_kf", "an_lower_bound_kf")
      .withColumn("study_id", lit(studyId))
      .withColumn("release_id", lit(releaseId))
      .withColumn("consent_codes_by_study", map($"study_id", $"consent_codes"))
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val tableVariants = tableName(destination.name, studyId, releaseId)
    data
      .repartition(col("chromosome"))
      .write.mode(SaveMode.Overwrite)
      .partitionBy("study_id", "release_id", "chromosome")
      .format("parquet")
      .option("path", s"${destination.rootPath}/${destination.name}/$tableVariants")
      .saveAsTable(s"$schema.$tableVariants")
    data
  }
}
