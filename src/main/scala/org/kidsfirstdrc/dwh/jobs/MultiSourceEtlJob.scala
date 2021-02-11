package org.kidsfirstdrc.dwh.jobs

import org.apache.spark.sql.functions.{col, lit, regexp_extract, trim}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Environment.Environment

import scala.util.Try

@deprecated("use org.kidsfirstdrc.dwh.jobs.DataSourceEtl instead")
abstract class MultiSourceEtlJob(runEnv: Environment) {

  val database: String
  val tableName: String

  private def regexp_extractFromCreateStatement[T](regex: String, defaultValue: T)(implicit spark: SparkSession): T = {
    Try {
      spark.sql(s"show create table $database.$tableName")
        .withColumn("extracted_value", regexp_extract(col("createtab_stmt"), regex, 1))
        .where(trim(col("extracted_value")) =!= lit(""))
        .select("extracted_value")
        .collect().head.getAs[T](0)
    }.getOrElse(defaultValue)
  }

  def lastReleaseId(implicit spark: SparkSession): String =
    regexp_extractFromCreateStatement("(re_\\d{6})", "re_000001")

  def lastTimestamp(implicit spark: SparkSession): String =
    regexp_extractFromCreateStatement("re_\\d{6}_(\\d{8}_\\d{6}$)", "")

  def extract(input: String)(implicit spark: SparkSession): Map[String, DataFrame]

  def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame

  def load(data: DataFrame, output: String)(implicit spark: SparkSession): DataFrame

  def run(input: String, output: String)(implicit spark: SparkSession): DataFrame = {
    val inputDF = extract(input)
    val outputDF = transform(inputDF)
    load(outputDF, output)
  }

}
