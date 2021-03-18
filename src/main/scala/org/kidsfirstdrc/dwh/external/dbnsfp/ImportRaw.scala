package org.kidsfirstdrc.dwh.external.dbnsfp

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.DataSource
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.CatalogV2.{Public, Raw}
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.StandardETL

class ImportRaw(runEnv: Environment)(implicit conf: Configuration)
  extends StandardETL(Public.dbnsfp_variant)(runEnv, conf) {

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    val dbnsfpDF =
      spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("nullValue", ".")
        .csv(Raw.dbNSFP_csv.location)
    Map(Raw.dbNSFP_csv -> dbnsfpDF)
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    data(Raw.dbNSFP_csv)
      .withColumnRenamed("#chr", "chr")
      .withColumnRenamed("position_1-based", "start")
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): Unit = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("parquet")
      .option("path", destination.location)
      .saveAsTable(s"${destination.database}.${destination.name}")
  }
}

