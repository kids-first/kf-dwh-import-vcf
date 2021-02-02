package org.kidsfirstdrc.dwh.utils

import org.apache.spark.sql.{DataFrame, SparkSession}


trait EtlJob {

  val database: String
  val tableName: String

  def extract(input: String)(implicit spark: SparkSession): DataFrame

  def transform(data: DataFrame)(implicit spark: SparkSession): DataFrame

  def load(data: DataFrame, output: String)(implicit spark: SparkSession): Unit

}