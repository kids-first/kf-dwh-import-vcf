package org.kidsfirstdrc.dwh.jobs

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.{DataSource, ETL}
import org.apache.spark.sql.functions.{col, lit, regexp_extract, trim}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.DataSourceImplicit._
import org.kidsfirstdrc.dwh.glue.UpdateTableComments

import scala.util.Try

abstract class StandardETL(override val destination: DataSource)
                          (implicit conf: Configuration) extends ETL(destination) {

  val view_db = "variant_live"

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("path", destination.location)
      .saveAsTable(s"${destination.database}.${destination.name}")
    data
  }

  override def publish()(implicit spark: SparkSession): Unit = {
    UpdateTableComments.run(destination.database, destination.name, destination.documentationPath)
    Try { spark.sql(s"drop table if exists $view_db.${destination.name}") }
    spark.sql(s"create or replace view $view_db.${destination.name} as select * from ${destination.database}.${destination.name}")
  }

  private def regexp_extractFromCreateStatement[T](regex: String, defaultValue: T)(implicit spark: SparkSession): T = {
    Try {
      spark.sql(s"show create table ${destination.database}.${destination.name}")
        .withColumn("extracted_value", regexp_extract(col("createtab_stmt"), regex, 1))
        .where(trim(col("extracted_value")) =!= lit(""))
        .select("extracted_value")
        .collect().head.getAs[T](0)
    }.getOrElse(defaultValue)
  }

  def lastReleaseId(implicit spark: SparkSession): String =
    regexp_extractFromCreateStatement("(re_\\d{6})", "re_000001")
}

