package org.kidsfirstdrc.dwh.external.dbnsfp

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.conf.Ds
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.DsETL

class ImportRaw(runEnv: Environment) extends DsETL(runEnv) {

  override val destination: Ds = Public.dbnsfp_variant

  override def extract()(implicit spark: SparkSession): Map[Ds, DataFrame] = {
    val dbnsfpDF =
      spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("nullValue", ".")
        .csv(Raw.dbNSFP_csv.path)
    Map(Raw.dbNSFP_csv -> dbnsfpDF)
  }

  override def transform(data: Map[Ds, DataFrame])(implicit spark: SparkSession): DataFrame = {
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

