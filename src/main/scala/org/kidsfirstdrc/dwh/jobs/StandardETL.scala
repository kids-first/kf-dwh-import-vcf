package org.kidsfirstdrc.dwh.jobs


import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.{DataSource, ETL}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Ds._
import org.kidsfirstdrc.dwh.conf.Environment
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.glue.UpdateTableComments

import scala.util.Try

abstract class StandardETL(override val destination: DataSource)
                          (implicit runEnv: Environment, conf: Configuration) extends ETL(destination) {

  val view_db = "variant_live"

  override def load(data: DataFrame)(implicit spark: SparkSession): Unit = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("path", destination.location)
      .saveAsTable(s"${destination.database}.${destination.name}")
  }

  override def publish()(implicit spark: SparkSession): Unit = {
    UpdateTableComments.run(destination.database, destination.name, destination.documentationPath)
    if (runEnv == Environment.PROD) {
      Try { spark.sql(s"drop table if exists $view_db.${destination.name}") }
      spark.sql(s"create or replace view $view_db.${destination.name} as select * from ${destination.database}.${destination.name}")
    }
  }

  override def run()(implicit spark: SparkSession): Unit = {
    val inputDF = extract()
    val outputDF = transform(inputDF)
    load(outputDF)
    publish()
  }
}

