package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object JoinWrite {

  def write(
      releaseId: String,
      output: String,
      tableName: String,
      df: DataFrame,
      nbPartitions: Option[Int],
      database: String
  )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    nbPartitions
      .map(df.repartitionByRange(_, $"chromosome", $"start"))
      .getOrElse(df.repartition($"chromosome"))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("parquet")
      .option("path", s"$output/$tableName/${tableName}_${releaseId.toLowerCase()}")
      .saveAsTable(s"${database}.${tableName}_${releaseId.toLowerCase()}")
    df
  }
}
