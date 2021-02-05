package org.kidsfirstdrc.dwh.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.glue.UpdateTableComments
import org.kidsfirstdrc.dwh.utils.Environment.Environment

abstract class DataSourceEtl(runEnv: Environment) {

  implicit val env: Environment = runEnv

  val target: DataSource

  def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame]

  def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame

  def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data.coalesce(1)
      .write
      .mode("overwrite")
      .format("parquet")
      .option("path", target.path)
      .saveAsTable(s"${target.database}.${target.name}")

    UpdateTableComments.run(target.database, target.name, target.documentationPath)
    if (runEnv == Environment.PROD) {
      spark.sql(s"create or replace view variant_live.${target.name} as select * from ${target.database}.${target.name}")
    }

    data
  }

  def run()(implicit spark: SparkSession): DataFrame = {
    val inputDF = extract()
    val outputDF = transform(inputDF)
    load(outputDF)
  }
}
