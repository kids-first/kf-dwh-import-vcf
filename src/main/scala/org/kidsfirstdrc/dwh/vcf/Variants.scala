package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.{DataSource, ETL}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.Clinical
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

class Variants(studyId: String, releaseId: String)(implicit conf: Configuration)
  extends ETL(Clinical.variants){

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(
      Clinical.occurrences -> spark.table(tableName(Clinical.occurrences.name, studyId, releaseId))
    )
  }

  override def run()(implicit spark: SparkSession): DataFrame = {
    val inputDF = extract()(spark)
    val variants: DataFrame = transform(inputDF)
    load(variants)
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(Clinical.occurrences)
      .select(
        $"chromosome",
        $"start",
        $"end",
        $"reference",
        $"alternate",
        $"name",
        $"zygosity",
        calculated_ac,
        calculate_an,
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
        firstAs("hgvsg") +:
          firstAs("end") +:
          firstAs("variant_class") +:
          collect_set($"dbgap_consent_code").as("consent_codes") +:
          (freqByDuoCode("hmb") ++ freqByDuoCode("gru")) :_*
      )
      .withColumn("hmb_af", calculated_duo_af("hmb"))
      .withColumn("gru_af", calculated_duo_af("gru"))
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
      .saveAsTable(tableVariants)
    data
  }

  def freqByDuoCode(duo: String): Seq[Column] = {
    Seq(
      sum(when(col(s"is_$duo"), col("ac")).otherwise(0)) as s"${duo}_ac",
      sum(when(col(s"is_$duo"), col("an")).otherwise(0)) as s"${duo}_an",
      sum(when(col(s"is_$duo"), col("homozygotes")).otherwise(0)) as s"${duo}_homozygotes",
      sum(when(col(s"is_$duo"), col("heterozygotes")).otherwise(0)) as s"${duo}_heterozygotes"
    )
  }
}
