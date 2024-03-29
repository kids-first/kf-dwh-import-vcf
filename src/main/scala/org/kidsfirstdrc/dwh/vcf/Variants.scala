package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RunType}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.implicits.SparkUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Raw}

import java.time.LocalDateTime

class Variants(studyId: String, releaseId: String, schema: String)(implicit conf: Configuration)
    extends ETL() {

  val destination: DatasetConf = Clinical.variants

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val participantsPath = Raw.all_participants.location
    val participantsDF = if(schema == "portal") spark.read.json(participantsPath) else spark.emptyDataFrame
    Map(
      Raw.all_participants.id -> participantsDF,
      Clinical.occurrences.id -> spark.table(tableName(Clinical.occurrences.id, studyId, releaseId))
    )
  }

  override def run(runType: RunType)(implicit spark: SparkSession): DataFrame = {
    val inputDF             = extract()(spark)
    val variants: DataFrame = transform(inputDF)
    load(variants)
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._


    val occurrences: DataFrame = schema match {
      case "portal" =>
        val participants = data(Raw.all_participants.id).select($"id" as "participant_id")
        data(Clinical.occurrences.id)
          .join(broadcast(participants), Seq("participant_id"), "inner")
      case _ => data(Clinical.occurrences.id)
    }

    val participantTotalCount = occurrences.select("participant_id").distinct().count()

    occurrences
      .filter($"has_alt" === 1)
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
        $"dbgap_consent_code",
        $"transmission"
      )
      .withColumn("variant_class",
        //VEP annotation code has a bug which sets some variant_class to 'indel' instead of 'SNV' when they contain only 1 nucleotide.
        //see here for more info about variant_class https://m.ensembl.org/info/genome/variation/prediction/classification.html#classes
        when(length($"reference") === 1 and length($"alternate") === 1, lit("SNV")).otherwise($"variant_class"))
      .withColumn("zygosity", when($"zygosity".isNull, lit("")).otherwise($"zygosity"))
      .withColumn("count", lit(1))
      .groupBy("transmission", locusColumNames:_*)
      .agg(
        sum("count") as "count_transmission",
        firstAs("name"),
        max("hgvsg") as "hgvsg",
        firstAs("end"),
        max("variant_class") as "variant_class",
        collect_set($"dbgap_consent_code").as("consent_codes"),
        sum(col("ac")) as "ac",
        sum(col("an_lower_bound_kf")) as "an_lower_bound_kf",
        sum(col("homozygotes")) as "homozygotes",
        sum(col("heterozygotes")) as "heterozygotes",
        collect_set("zygosity") as "zygosity"
      )
      .groupBy(locus: _*)
      .agg(
        map_from_entries(filter(collect_list(struct(col("transmission"), col("count_transmission"))), c => c("transmission").isNotNull)) as "transmissions",
        firstAs("name"),
        max("hgvsg") as "hgvsg",
        firstAs("end"),
        max("variant_class") as "variant_class",
        array_distinct(flatten(collect_list($"consent_codes"))).as("consent_codes"),
        sum(col("ac")) as "ac",
        sum(col("an_lower_bound_kf")) as "an_lower_bound_kf",
        sum(col("homozygotes")) as "homozygotes",
        sum(col("heterozygotes")) as "heterozygotes",
        array_remove(array_distinct(flatten(collect_list("zygosity"))), "") as "zygosity"
      )
      .withColumn("an_upper_bound_kf", calculate_an_upper_bound_kf(participantTotalCount))
      .withColumn(
        "frequencies",
        struct(
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
          ) as "lower_bound_kf"
        )
      )
      .drop(
        "an",
        "ac",
        "af",
        "heterozygotes",
        "homozygotes",
        "an_upper_bound_kf",
        "an_lower_bound_kf",
        "count"
      )
      .withColumn("study_id", lit(studyId))
      .withColumn("release_id", lit(releaseId))
      .withColumn("consent_codes_by_study", map($"study_id", $"consent_codes"))
      .withColumn("transmissions_by_study", map($"study_id", $"transmissions"))
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val tableVariants = tableName(destination.id, studyId, releaseId)
    data
      .repartition(col("chromosome"))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("study_id", "release_id", "chromosome")
      .format("parquet")
      .option("path", s"${destination.rootPath}/${destination.id}/$tableVariants")
      .saveAsTable(s"$schema.$tableVariants")
    data
  }
}
