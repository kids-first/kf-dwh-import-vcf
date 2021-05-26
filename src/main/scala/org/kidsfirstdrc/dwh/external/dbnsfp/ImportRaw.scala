package org.kidsfirstdrc.dwh.external.dbnsfp

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.config.DatasetConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.jobs.StandardETL

class ImportRaw()(implicit conf: Configuration)
  extends StandardETL(Public.dbnsfp_variant)(conf) {

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    val dbnsfpDF =
      spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("nullValue", ".")
        .csv(Raw.dbNSFP_csv.location)
    Map(Raw.dbNSFP_csv.id -> dbnsfpDF)
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    data(Raw.dbNSFP_csv.id)
      .withColumnRenamed("#chr", "chr")
      .withColumnRenamed("position_1-based", "start")
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("parquet")
      .option("path", destination.location)
      .saveAsTable(s"${destination.table.get.fullName}")
    data
  }
}

