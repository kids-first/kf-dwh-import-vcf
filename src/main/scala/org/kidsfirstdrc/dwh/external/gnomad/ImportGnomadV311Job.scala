package org.kidsfirstdrc.dwh.external.gnomad

import bio.ferlab.datalake.spark3.config.Configuration
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.conf.Catalog.Raw.gnomad_genomes_3_1_1
import org.kidsfirstdrc.dwh.jobs.StandardETL
import org.kidsfirstdrc.dwh.utils.SparkUtils.escapeInfoAndLowercase
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
import org.kidsfirstdrc.dwh.utils.SparkUtils.vcf

class ImportGnomadV311Job(implicit conf: Configuration)
  extends StandardETL(Public.gnomad_genomes_3_1_1)(conf) {

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(gnomad_genomes_3_1_1.id -> vcf(gnomad_genomes_3_1_1.location))
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val df = data(gnomad_genomes_3_1_1.id)

    df
      .select(
        chromosome +:
          start +:
          end +:
          reference +:
          alternate +:
          (col("qual") as "qual") +:
          name +:
          escapeInfoAndLowercase(df): _*)
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(col("chromosome"))
      .sortWithinPartitions("start"))
  }
}
