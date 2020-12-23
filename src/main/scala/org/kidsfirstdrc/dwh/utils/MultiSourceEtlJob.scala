package org.kidsfirstdrc.dwh.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 */
trait MultiSourceEtlJob {

  def extract(input: String)(implicit spark: SparkSession): Map[String, DataFrame]

  def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame

  def load(data: DataFrame, output: String)(implicit spark: SparkSession): Unit

}
