package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.commons.config.Configuration
import io.projectglow.functions.lift_over_coordinates
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.jobs.StandardETL

import java.time.LocalDateTime

class ImportCancerHotspots()(implicit conf: Configuration)
    extends StandardETL(Public.cancer_hotspots)(conf) with App {
  val chain = "/home/hadoop/b37ToHg38.over.chain"

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val df = spark.read
      .option("comment", "#")
      .option("header", "true")
      .option("sep", "\t")
      .csv(Raw.cancerhotspots_csv.location)
    Map(Raw.cancerhotspots_csv.id -> df)
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val lifted: Dataset[Row] =
      data(Raw.cancerhotspots_csv.id)
        .withColumn("end", $"End_Position" + 1)
        .drop($"End_Position")
        .withColumn(
          "lifted",
          lift_over_coordinates($"Chromosome", $"Start_Position", $"end", chain, 0.90)
        )
        .drop("Chromosome", "Start_Position", "end")
        .where($"lifted".isNotNull)
    lifted
      .select(
        ltrim(col("lifted.contigName"), "chr").as("chromosome") ::
          $"lifted.start" ::
          $"lifted.end" ::
          lifted.columns.collect {
            case x
                if x != "AF" && !x.contains("_AF") && !x.startsWith("ExAC_") && !x
                  .startsWith("gnomAD_") =>
              col(x).as(x.toLowerCase)
          }.toList: _*
      )
      .drop("lifted")
  }
}
