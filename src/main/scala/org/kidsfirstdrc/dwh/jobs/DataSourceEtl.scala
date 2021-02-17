package org.kidsfirstdrc.dwh.jobs

import org.apache.spark.sql.functions.{col, lit, regexp_extract, trim}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.conf.{DataSource, Environment}
import org.kidsfirstdrc.dwh.glue.UpdateTableComments

import scala.util.Try

abstract class DataSourceEtl(runEnv: Environment) {

  implicit val env: Environment = runEnv

  val destination: DataSource

  def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame]

  def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame

  def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("path", destination.path(runEnv))
      .saveAsTable(s"${destination.database}.${destination.name}")
    data
  }

  def publish(view_db: String)(implicit spark: SparkSession): Unit = {
    UpdateTableComments.run(destination)
    if (runEnv == Environment.PROD) {
      Try { spark.sql(s"drop table if exists $view_db.${destination.name}") }
      spark.sql(s"create or replace view $view_db.${destination.name} as select * from ${destination.database}.${destination.name}")
    }
  }

  def run()(implicit spark: SparkSession): DataFrame = {
    val inputDF = extract()
    val outputDF = transform(inputDF)
    load(outputDF)
    publish(view_db = "variant_live")
    outputDF
  }

  private def regexp_extractFromCreateStatement[T](regex: String, defaultValue: T)(implicit spark: SparkSession): T = {
    Try {
      spark.sql(s"show create table ${destination.database}.${destination.name}")
        .withColumn("extracted_value", regexp_extract(col("createtab_stmt"), regex, 1))
        .where(trim(col("extracted_value")) =!= lit(""))
        .select("extracted_value")
        .collect().head.getAs[T](0)
    }.getOrElse(defaultValue)
  }

  def lastReleaseId(implicit spark: SparkSession): String =
    regexp_extractFromCreateStatement("(re_\\d{6})", "re_000001")
}
