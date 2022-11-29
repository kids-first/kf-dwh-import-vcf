package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RunStep}
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.functions.{col, sha1}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Es, Public}

import java.time.LocalDateTime

class GeneCentricIndex(releaseId: String)
                      (override implicit val conf: Configuration) extends ETL() {

  override val destination: DatasetConf = Es.gene_centric

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      Public.genes.id -> spark.table(s"${Public.genes.table.get.fullName}")
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    data(Public.genes.id)
      .withColumn("hash", sha1(col("symbol")))
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .option("format", destination.format.sparkFormat)
      .option("path", s"${destination.location}_$releaseId")
      .saveAsTable(s"${destination.table.get.fullName}_$releaseId")
    data
  }

  override def run(runSteps: Seq[RunStep] = RunStep.default_load,
                   lastRunDateTime: Option[LocalDateTime] = None,
                   currentRunDateTime: Option[LocalDateTime] = None)(implicit spark: SparkSession): DataFrame = {
    val inputDF  = extract()
    val outputDF = transform(inputDF).persist()
    println(s"count: ${outputDF.count}")
    println(s"distinct symbol: ${outputDF.dropDuplicates("symbol").count()}")
    load(outputDF)
  }
}
