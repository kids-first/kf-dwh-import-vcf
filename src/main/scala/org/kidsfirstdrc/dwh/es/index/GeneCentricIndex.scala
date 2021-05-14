package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.spark3.config.{Configuration, SourceConf}
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.functions.{col, sha1}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Es, Public}

class GeneCentricIndex()(override implicit val conf: Configuration) extends ETL() {

  val destination = Es.gene_centric

  override def extract()(implicit spark: SparkSession): Map[SourceConf, DataFrame] = {
    Map(
      Public.genes -> spark.table(s"${Public.genes.database}.${Public.genes.name}")
    )
  }

  override def transform(data: Map[SourceConf, DataFrame])(implicit spark: SparkSession): DataFrame = {
    data(Public.genes)
      .withColumn("hash", sha1(col("symbol")))
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${destination.location}")
    data
  }

  override def run()(implicit spark: SparkSession): DataFrame = {
    val inputDF = extract()
    val outputDF = transform(inputDF).persist()
    println(s"count: ${outputDF.count}")
    println(s"distinct symbol: ${outputDF.dropDuplicates("symbol").count()}")
    load(outputDF)
  }
}
