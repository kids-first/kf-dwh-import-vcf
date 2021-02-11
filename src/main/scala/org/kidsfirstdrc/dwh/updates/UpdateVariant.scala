package org.kidsfirstdrc.dwh.updates

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public}
import org.kidsfirstdrc.dwh.conf.Environment.{Environment, LOCAL}
import org.kidsfirstdrc.dwh.conf.{DataSource, Environment}
import org.kidsfirstdrc.dwh.jobs.DataSourceEtl
import org.kidsfirstdrc.dwh.join.JoinWrite.write
import org.kidsfirstdrc.dwh.publish.Publish.publishTable
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locusColumNames

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class UpdateVariant(source: DataSource, runEnv: Environment) extends DataSourceEtl(runEnv) {

  override val destination: DataSource = Clinical.variants

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(
      destination -> spark.table(s"${destination.database}.${destination.name}"),
      //TODO remove .dropDuplicates(locusColumNames) when issue#2893 is fixed
      source -> spark.table(s"variant.${source.name}").dropDuplicates(locusColumNames),
    )
  }

  private def updateClinvar(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val variant = data(destination).drop("clinvar_id", "clin_sig")
    val clinvar = data(Public.clinvar)
    variant
      .joinByLocus(clinvar)
      .select(variant("*"), clinvar("name") as "clinvar_id", clinvar("clin_sig") as "clin_sig")
  }

  private def updateTopmed(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val variant = data(destination).drop("clinvar_id", "clin_sig")
    val topmed = data(Public.topmed_bravo)
    variant
      .drop("topmed")
      .joinAndMerge(topmed, "topmed")
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    source match {
      case Public.clinvar => updateClinvar(data)
      case Public.topmed_bravo => updateTopmed(data)
      case _ => throw new IllegalArgumentException(s"Nothing to do for $source")
    }
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val localTimeNow = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val releaseId_datetime = s"${lastReleaseId}_$localTimeNow"

    if (runEnv == LOCAL)
      write(releaseId_datetime, getClass.getClassLoader.getResource("tables").getFile, destination.name, data, Some(60), destination.database)
    else
      write(releaseId_datetime, destination.bucket, destination.name, data, Some(60), destination.database)
    if (runEnv == Environment.PROD || runEnv == Environment.LOCAL) publishTable(releaseId_datetime, destination.name)
    data
  }
}
