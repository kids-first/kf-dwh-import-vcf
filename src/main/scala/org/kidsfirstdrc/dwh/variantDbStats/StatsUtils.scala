package org.kidsfirstdrc.dwh.variantDbStats

import org.apache.spark.sql.functions.not
import org.apache.spark.sql.{DataFrame, SparkSession}

object StatsUtils {

  def getOccurrencesTableWORelease(database: String)(implicit spark: SparkSession): Array[String] = {
    import spark.implicits._

    spark
      .sql(s"show tables in $database").where($"tableName" like "occurrences_sd_%" and not($"tableName" like "%_re_00%")  )
      .select("tableName").collect().flatMap(_.toSeq).map(_.toString)
  }

  def getUnionOfOccurrences(database: String, tabledList: Array[String])(implicit spark: SparkSession): DataFrame = {
    val occurrences = tabledList.map{table =>
      spark.table(s"$database.$table")
    }

    occurrences.reduce(_ union _)
  }

  def getStats(df: DataFrame): VariantDbStats = {
    val studiesCount = df.select("study_id").distinct.count()
    val participantsCount = df.select("participant_id").distinct().count()
    val distinctVariantsCount = df.where("has_alt").select("chromosome", "start", "reference", "alternate").distinct.count()
    val occurrencesCount = df.where("has_alt").count()


    VariantDbStats(studiesCount, participantsCount, distinctVariantsCount, occurrencesCount)
  }

}
