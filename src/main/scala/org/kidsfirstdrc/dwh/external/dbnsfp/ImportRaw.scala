package org.kidsfirstdrc.dwh.external.dbnsfp

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.conf.DataSource
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.DataSourceEtl

class ImportRaw(runEnv: Environment) extends DataSourceEtl(runEnv) {

  override val destination: DataSource = Public.dbnsfp_variant

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    val dbnsfpDF =
      spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("nullValue", ".")
        .csv(Raw.dbNSFP_csv.path)
    Map(Raw.dbNSFP_csv -> dbnsfpDF)
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
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
      .option("path", destination.path)
      .saveAsTable(s"${destination.database}.${destination.name}")
    data
  }
}

