package org.kidsfirstdrc.dwh.updates

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.join.JoinWrite.write
import org.kidsfirstdrc.dwh.publish.Publish.publishTable
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._
import org.kidsfirstdrc.dwh.utils.Environment.Environment
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locusColumNames
import org.kidsfirstdrc.dwh.utils.{Environment, MultiSourceEtlJob}
import org.kidsfirstdrc.dwh.vcf.Variants

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class UpdateVariant(runEnv: Environment) extends MultiSourceEtlJob(runEnv) {

  override val database: String = "variant"
  override val tableName: String = "variants"

  override def extract(input: String)(implicit spark: SparkSession): Map[String, DataFrame] = {

    val path = lastTimestamp match {
      case ""        => s"$input/variants/${Variants.TABLE_NAME}_${lastReleaseId}"
      case timestamp => s"$input/variants/${Variants.TABLE_NAME}_${lastReleaseId}_${timestamp}"
    }

    Map(
      Variants.TABLE_NAME -> spark.read.parquet(path),
      //TODO remove .dropDuplicates(locusColumNames) when issue#2893 is fixed
      "clinvar" -> spark.table(s"$database.clinvar").dropDuplicates(locusColumNames)
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val variant = data(Variants.TABLE_NAME).drop("clinvar_id", "clin_sig")
    val clinvar = data("clinvar")
    variant
      .joinByLocus(clinvar)
      .select(variant("*"), clinvar("name") as "clinvar_id", clinvar("clin_sig") as "clin_sig")
  }

  override def load(data: DataFrame, output: String)(implicit spark: SparkSession): DataFrame = {

    val localTimeNow = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val releaseId_datetime = s"${lastReleaseId}_$localTimeNow"

    write(releaseId_datetime, output, Variants.TABLE_NAME, data, Some(60), database)
    if (runEnv.equals(Environment.PROD)) publishTable(releaseId_datetime, Variants.TABLE_NAME)
    data
  }

  def run(inputFolder: String, outputFolder: String)(implicit spark: SparkSession): DataFrame = {
    val inputDF = extract(inputFolder)
    val outputDF = transform(inputDF)
    load(outputDF, outputFolder)
  }
}
