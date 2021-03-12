package org.kidsfirstdrc.dwh.es.stats

import org.apache.spark.sql.functions.not
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StatsUtils {

  /**
   * Fetch all Occurrences tables in a specific database that do not contain a release number
   * @param database database where to do the lookup
   * @param spark spark session
   * @return an array of table names that satisfy the predicate
   */
  def getOccurrencesTableWORelease(database: String)(implicit spark: SparkSession): Array[String] = {
    import spark.implicits._

    spark
      .sql(s"show tables in $database").where($"tableName" like "occurrences_sd_%" and not($"tableName" like "occurrences_sd_%_re_%")  )
      .select("tableName").collect().flatMap(_.toSeq).map(_.toString)
  }

  def getUnionOfOccurrences(database: String, tabledList: Array[String])(implicit spark: SparkSession): DataFrame = {
    val occurrences = tabledList.map{table =>
      spark.table(s"$database.$table")
    }

    occurrences.reduce(_ unionByName _)
  }

  def getStats(df: DataFrame): VariantDbStats = {
    val studiesCount = df.select("study_id").distinct.count()
    val participantsCount = df.select("participant_id").distinct().count()
    val distinctVariantsCount = df.where(col("has_alt") === 1).select("chromosome", "start", "reference", "alternate").distinct.count()
    val occurrencesCount = df.where(col("has_alt") === 1).count()


    VariantDbStats(studiesCount, participantsCount, distinctVariantsCount, occurrencesCount)
  }

}
