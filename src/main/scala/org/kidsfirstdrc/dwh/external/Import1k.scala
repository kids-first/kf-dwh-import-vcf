package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.spark3.config.Configuration
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog._
import org.kidsfirstdrc.dwh.jobs.StandardETL

class Import1k()(implicit conf: Configuration) extends StandardETL(Public.`1000_genomes`)(conf) {

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(Raw.`1000genomes_vcf`.id -> vcf(Raw.`1000genomes_vcf`.location, None))
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    data(Raw.`1000genomes_vcf`.id)
      .select(
        chromosome,
        start,
        end,
        name,
        reference,
        alternate,
        ac,
        af,
        an,
        afr_af,
        eur_af,
        sas_af,
        amr_af,
        eas_af,
        dp
      )
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(col("chromosome"))
      .sortWithinPartitions("start"))
  }
}
