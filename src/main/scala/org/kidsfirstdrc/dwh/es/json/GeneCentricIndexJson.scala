package org.kidsfirstdrc.dwh.es.json

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.{DataSource, ETL}
import org.apache.spark.sql.functions.{col, sha1}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.es.json.EsCatalog.Public

class GeneCentricIndexJson()(override implicit val conf: Configuration) extends ETL(EsCatalog.Es.gene_centric) {

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(
      Public.genes -> spark.table(s"${Public.genes.database}.${Public.genes.name}")
    )
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    data(Public.genes)
      .withColumn("hash", sha1(col("symbol")))
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): Unit = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .format("json")
      .json(s"${destination.location}")
  }

  override def run()(implicit spark: SparkSession): Unit = {
    val inputDF = extract()
    val outputDF = transform(inputDF).persist()
    println(s"count: ${outputDF.count}")
    println(s"distinct symbol: ${outputDF.dropDuplicates("symbol").count()}")
    load(outputDF)
  }
}
