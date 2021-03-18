package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.DataSource
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.CatalogV2._
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.StandardETL
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

class Import1k(runEnv: Environment)(implicit conf: Configuration)
  extends StandardETL(Public.`1000_genomes`)(runEnv, conf) {

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(Raw.`1000genomes_vcf` -> vcf(Raw.`1000genomes_vcf`.location))
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    data(Raw.`1000genomes_vcf`)
      .select(chromosome,
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

  override def load(data: DataFrame)(implicit spark: SparkSession): Unit = {
    data
      .repartition(col("chromosome"))
      .sortWithinPartitions("start")
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("path", destination.location)
      .saveAsTable(s"${destination.database}.${destination.name}")
  }
}
