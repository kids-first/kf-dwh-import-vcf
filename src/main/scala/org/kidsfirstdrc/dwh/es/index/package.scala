package org.kidsfirstdrc.dwh.es

import bio.ferlab.datalake.commons.config.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.kidsfirstdrc.dwh.conf.Catalog.Clinical

import scala.util.{Success, Try}

package object index {

  def getOccurrencesWithAlt(schema: String, releaseId: String)(implicit spark: SparkSession, conf: Configuration): DataFrame = {
    import spark.implicits._
    spark
      .table(s"variants_$releaseId")
      .withColumn("study", explode(col("studies")))
      .select("study")
      .distinct
      .as[String]
      .collect()
      .map(studyId =>
        Try(
          spark.table(s"$schema.occurrences_${studyId.toLowerCase}")
        )
      )
      .collect { case Success(df) => df }
      .reduce(_ unionByName _)
      .where(col("has_alt") === 1)
  }
}
