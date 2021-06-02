package org.kidsfirstdrc.dwh.external.gnomad

import bio.ferlab.datalake.spark3.config.Configuration
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.jobs.StandardETL
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
import org.kidsfirstdrc.dwh.utils.SparkUtils.vcf

class ImportGnomadV311Job(implicit conf: Configuration) extends StandardETL(Public.gnomad_genomes_3_1_1)(conf) {

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(Raw.gnomad_genomes_3_1_1_vcf.id -> vcf(Raw.gnomad_genomes_3_1_1_vcf.location))
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val df = data(Raw.gnomad_genomes_3_1_1_vcf.id)

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
        .repartitionByRange(1000, col("chromosome"), col("start"))
    )
  }

  /* in case we decide to load the files one at a time
   *
  override def run()(implicit spark: SparkSession): DataFrame = {
    //clears the existing data
    HadoopFileSystem.remove(destination.location)
    //for each file found in /raw/gnomad/r3.1.1/
    HadoopFileSystem
      .list(Raw.gnomad_genomes_3_1_1.location, recursive = true)
      .filter(_.name.endsWith(".vcf.gz"))
      .foreach { f =>
        println(s"processing ${f.path}")
        val input = Map(Raw.gnomad_genomes_3_1_1.id -> vcf(f.path))
        load(transform(input))
        println(s"Done")
      }
    spark.emptyDataFrame
  }
   */
}
