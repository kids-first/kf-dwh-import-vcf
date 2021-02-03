package org.kidsfirstdrc.dwh.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.utils.Environment.Environment

abstract class DataSourceEtl(runEnv: Environment) {

  val target: DataSource

  def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame]

  def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame

  def load(data: DataFrame)(implicit spark: SparkSession): DataFrame

  def run()(implicit spark: SparkSession): DataFrame = {
    val inputDF = extract()
    val outputDF = transform(inputDF)
    load(outputDF)
  }
}
