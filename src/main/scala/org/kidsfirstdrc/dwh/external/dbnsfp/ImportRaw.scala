package org.kidsfirstdrc.dwh.external.dbnsfp

import bio.ferlab.datalake.spark3.config.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.jobs.StandardETL

import java.time.LocalDateTime

class ImportRaw()(implicit conf: Configuration) extends StandardETL(Public.dbnsfp_variant)(conf) {

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val dbnsfpDF =
      spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("nullValue", ".")
        .csv(Raw.dbNSFP_csv.location)
    Map(Raw.dbNSFP_csv.id -> dbnsfpDF)
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    data(Raw.dbNSFP_csv.id)
      .withColumnRenamed("#chr", "chr")
      .withColumnRenamed("position_1-based", "start")
  }
}
