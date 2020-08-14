package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

object JoinWrite {

  def write(releaseId: String, output: String, tableName: String, df: DataFrame, nbFile: Int)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    if (nbFile == 1) {
      df.repartition($"chromosome")
        .sortWithinPartitions("start")
        .write.mode(SaveMode.Overwrite)
        .partitionBy("chromosome")
        .format("parquet")
        .option("path", s"$output/consequences/consequences_$releaseId")
        .saveAsTable(s"consequences_$releaseId")
    } else {
      df
        .withColumn("bucket",
          functions
            .ntile(nbFile)
            .over(
              Window.partitionBy("chromosome")
                .orderBy("start")
            )
        )
        .repartition($"chromosome", $"bucket")
        .sortWithinPartitions($"chromosome", $"bucket", $"start").write.mode(SaveMode.Overwrite)
        .partitionBy("chromosome")
        .format("parquet")
        .option("path", s"$output/$tableName/${tableName}_$releaseId")
        .saveAsTable(s"${tableName}_$releaseId")
    }
  }
}
