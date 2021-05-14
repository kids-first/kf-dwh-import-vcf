package org.kidsfirstdrc.dwh.external.dbnsfp

import bio.ferlab.datalake.spark3.config.{Configuration, SourceConf}
import bio.ferlab.datalake.spark3.config.SourceConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.jobs.StandardETL

class ImportRaw()(implicit conf: Configuration)
  extends StandardETL(Public.dbnsfp_variant)(conf) {

  override def extract()(implicit spark: SparkSession): Map[SourceConf, DataFrame] = {
    val dbnsfpDF =
      spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("nullValue", ".")
        .csv(Raw.dbNSFP_csv.location)
    Map(Raw.dbNSFP_csv -> dbnsfpDF)
  }

  override def transform(data: Map[SourceConf, DataFrame])(implicit spark: SparkSession): DataFrame = {
    data(Raw.dbNSFP_csv)
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
      .saveAsTable(s"${destination.database}.${destination.name}")
    data
  }
}

