package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.conf.Ds
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.DsETL
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

class ImportDBSNP(runEnv: Environment) extends DsETL(runEnv) with App {

  override val destination = Public.dbsnp

  override def extract()(implicit spark: SparkSession): Map[Ds, DataFrame] = {
    Map(Raw.dbsnp_vcf -> vcf(Raw.dbsnp_vcf.path))
  }

  override def transform(data: Map[Ds, DataFrame])(implicit spark: SparkSession): DataFrame = {
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
      .option("path", destination.path)
      .saveAsTable(s"${destination.database}.${destination.name}")
    data
  }
}
