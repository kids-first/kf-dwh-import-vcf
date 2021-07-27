package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.spark3.config.Configuration
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.jobs.StandardETL

class ImportDBSNP()(implicit conf: Configuration) extends StandardETL(Public.dbsnp)(conf) {

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(Raw.dbsnp_vcf.id -> vcf(Raw.dbsnp_vcf.location, None))
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(Raw.dbsnp_vcf.id)
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
    super.load(data
      .repartition($"chromosome")
      .sortWithinPartitions("start"))
  }
}
