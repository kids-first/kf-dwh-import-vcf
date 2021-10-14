package org.kidsfirstdrc.dwh.jobs

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, regexp_extract, trim}
import org.kidsfirstdrc.dwh.glue.UpdateTableComments

import scala.util.Try

abstract class StandardETL(val destination: DatasetConf)(implicit conf: Configuration)
    extends ETL() {

  val view_db = "variant_live"

  override def publish()(implicit spark: SparkSession): Unit = {
    UpdateTableComments.run(destination)
    Try { spark.sql(s"drop table if exists $view_db.${destination.table.get.name}") }
    spark.sql(
      s"create or replace view $view_db.${destination.table.get.name} as select * from ${destination.table.get.fullName}"
    )
  }

  private def regexp_extractFromCreateStatement[T](regex: String, defaultValue: T)(implicit
      spark: SparkSession
  ): T = {
    Try {
      spark
        .sql(s"show create table ${destination.table.get.fullName}")
        .withColumn("extracted_value", regexp_extract(col("createtab_stmt"), regex, 1))
        .where(trim(col("extracted_value")) =!= lit(""))
        .select("extracted_value")
        .collect()
        .head
        .getAs[T](0)
    }.getOrElse(defaultValue)
  }

  def lastReleaseId(implicit spark: SparkSession): String =
    regexp_extractFromCreateStatement("(re_\\d{6})", "re_000001")
}