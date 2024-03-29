package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.commons.config.{Configuration, RunType}
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.functions.{col, sha1, array, filter, array_union, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Es, Public}

import java.time.LocalDateTime

class GeneCentricIndex(releaseId: String)(override implicit val conf: Configuration) extends ETL() {

  val destination = Es.gene_centric

  override def extract(
      lastRunDateTime: LocalDateTime = minDateTime,
      currentRunDateTime: LocalDateTime = LocalDateTime.now()
  )(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      Public.genes.id -> spark.table(s"${Public.genes.table.get.fullName}")
    )
  }

  override def transform(
      data: Map[String, DataFrame],
      lastRunDateTime: LocalDateTime = minDateTime,
      currentRunDateTime: LocalDateTime = LocalDateTime.now()
  )(implicit spark: SparkSession): DataFrame = {
    data(Public.genes.id)
      .withColumn("hash", sha1(col("symbol")))
      .withColumn(
        "search_text",
        filter(
          // Note: array_union([a, b], null) => null whereas array_union([a, b], []) => [a, b]
          array_union(
            array(col("symbol"), col("ensembl_gene_id")),
            // falling back on [] if no "alias" so that we have: array_union([a, b], []) => [a, b]
            when(col("alias").isNotNull, col("alias")).otherwise(array().cast("array<string>"))
          ),
          x => x.isNotNull && x =!= ""
        )
      )
  }

  override def load(
      data: DataFrame,
      lastRunDateTime: LocalDateTime = minDateTime,
      currentRunDateTime: LocalDateTime = LocalDateTime.now()
  )(implicit spark: SparkSession): DataFrame = {
    data.write
      .mode(SaveMode.Overwrite)
      .option("format", destination.format.sparkFormat)
      .option("path", s"${destination.location}_$releaseId")
      .saveAsTable(s"${destination.table.get.fullName}_$releaseId")
    data
  }

  override def run(runType: RunType)(implicit spark: SparkSession): DataFrame = {
    val inputDF  = extract()
    val outputDF = transform(inputDF).persist()
    println(s"count: ${outputDF.count}")
    println(s"distinct symbol: ${outputDF.dropDuplicates("symbol").count()}")
    load(outputDF)
  }
}
