package org.kidsfirstdrc.dwh.external.gnomad

import bio.ferlab.datalake.spark3.config.Configuration
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.conf.Catalog.Raw.gnomad_genomes_3_1_1
import org.kidsfirstdrc.dwh.jobs.StandardETL
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
import org.kidsfirstdrc.dwh.utils.SparkUtils.vcf

class ImportGnomadV311Job(implicit conf: Configuration)
    extends StandardETL(Public.gnomad_genomes_3_1_1)(conf) {

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(gnomad_genomes_3_1_1.id -> vcf(gnomad_genomes_3_1_1.location))
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val df = data(gnomad_genomes_3_1_1.id)

    df
      .select(
        chromosome +:
          start +:
          end +:
          reference +:
          alternate +:
          $"qual" +:
          name +:
          flattenInfo(df): _*
      )
  }

  private def flattenInfo(df: DataFrame): Seq[Column] = {
    val replaceColumnName: String => String = name => name.replace("INFO_", "").toLowerCase

    df.schema.toList.collect {
      case c
          if (c.name.startsWith("INFO_AN") ||
            c.name.startsWith("INFO_AC") ||
            c.name.startsWith("INFO_AF") ||
            c.name.startsWith("INFO_nhomalt")) && c.dataType.isInstanceOf[ArrayType] =>
        col(c.name)(0) as replaceColumnName(c.name)
      case c if c.name.startsWith("INFO_") =>
        col(c.name) as replaceColumnName(c.name)
    }
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(
      data
        .repartition(col("chromosome"))
        .sortWithinPartitions("start")
    )
  }
}
