package org.kidsfirstdrc.dwh.external

import io.projectglow.functions.lift_over_coordinates
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.conf.Ds
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.DsETL

class ImportCancerHotspots(runEnv: Environment) extends DsETL(runEnv) with App {
  val chain = "/home/hadoop/b37ToHg38.over.chain"
  override val destination = Public.cancer_hotspots

  override def extract()(implicit spark: SparkSession): Map[Ds, DataFrame] = {
    val df = spark.read
      .option("comment", "#")
      .option("header", "true")
      .option("sep", "\t").csv(Raw.cancerhotspots_csv.path)
    Map(Raw.cancerhotspots_csv -> df)
  }

  override def transform(data: Map[Ds, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val lifted: Dataset[Row] =
      data(Raw.cancerhotspots_csv)
        .withColumn("end", $"End_Position" + 1)
        .drop($"End_Position")
        .withColumn("lifted", lift_over_coordinates($"Chromosome", $"Start_Position", $"end", chain, 0.90))
        .drop("Chromosome", "Start_Position", "end")
        .where($"lifted".isNotNull)
    lifted
      .select(
        ltrim(col("lifted.contigName"), "chr").as("chromosome") ::
          $"lifted.start"::
          $"lifted.end" ::
        lifted.columns.collect { case x if x != "AF" && !x.contains("_AF") && !x.startsWith("ExAC_") && !x.startsWith("gnomAD_") => col(x).as(x.toLowerCase) }.toList: _*)
      .drop("lifted")
  }
}
