package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.DataSource
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.StandardETL
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

class ImportDBSNP(runEnv: Environment)(implicit conf: Configuration)
  extends StandardETL(Public.dbsnp)(runEnv, conf) {

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(Raw.dbsnp_vcf -> vcf(Raw.dbsnp_vcf.location))
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(Raw.dbsnp_vcf)
      .where($"contigName" like "NC_%")
      .withColumn("chromosome", regexp_extract($"contigName", "NC_(\\d+).(\\d+)", 1).cast("int"))
      .select(
        when($"chromosome" === 23, "X")
          .when($"chromosome" === 24, "Y")
          .when($"chromosome" === 12920, "M")
          .otherwise($"chromosome".cast("string")) as "chromosome",
        start,
        end,
        name,
        reference,
        alternate,
        $"contigName" as "original_contig_name"
      )
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data
      .repartition($"chromosome")
      .sortWithinPartitions("start")
      .write
      .partitionBy("chromosome")
      .mode(SaveMode.Overwrite)
      .format(destination.format.sparkFormat)
      .option("path", destination.location)
      .saveAsTable(s"${destination.database}.${destination.name}")
    data
  }
}
