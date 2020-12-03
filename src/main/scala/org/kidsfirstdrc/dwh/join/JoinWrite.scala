package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

object JoinWrite {

  def write(releaseId: String, output: String, tableName: String, df: DataFrame, nbPartitions: Option[Int], database: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    nbPartitions.map( df.repartitionByRange(_, $"chromosome", $"start"))
      .getOrElse(df.repartition($"chromosome"))
      .write.mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("parquet")
      .option("path", s"$output/$tableName/${tableName}_$releaseId")
      .saveAsTable(s"${database}.${tableName}_$releaseId")

  }
}
