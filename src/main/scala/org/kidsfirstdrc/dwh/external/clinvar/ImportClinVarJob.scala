package org.kidsfirstdrc.dwh.external.clinvar

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.implicits.SparkUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.jobs.StandardETL

import java.time.LocalDateTime
import scala.collection.mutable

class ImportClinVarJob()(implicit conf: Configuration) extends StandardETL(Public.clinvar)(conf) {

  val clinvar_vcf: DatasetConf = conf.getDataset("clinvar_vcf")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {

    Map(clinvar_vcf.id -> vcf(clinvar_vcf.location, None))
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {

    val df = data(clinvar_vcf.id)
    spark.udf.register("inheritance", inheritance_udf)

    val intermediateDf =
      df
        .select(
          chromosome +:
            start +:
            end +:
            name +:
            reference +:
            alternate +:
            (col("INFO_CLNSIG") as "clin_sig") +:
            (col("INFO_CLNSIGCONF") as "clin_sig_conflict") +:
            escapeInfoAndLowercase(df, "INFO_CLNSIG", "INFO_CLNSIGCONF"): _*
        )
        .withColumn(
          "clin_sig",
          split(regexp_replace(concat_ws("|", col("clin_sig")), "^_|\\|_|/", "|"), "\\|")
        )
        .withColumn(
          "clnrevstat",
          split(regexp_replace(concat_ws("|", col("clnrevstat")), "^_|\\|_|/", "|"), "\\|")
        )
        .withColumn(
          "clin_sig_conflict",
          split(
            regexp_replace(concat_ws("|", col("clin_sig_conflict")), "\\(\\d{1,2}\\)", ""),
            "\\|"
          )
        )

    intermediateDf.withInterpretations
      .withColumn("clndisdb", split(concat_ws("|", col("clndisdb")), "\\|"))
      .withColumn("clndn", split(concat_ws("", col("clndn")), "\\|"))
      .withColumn(
        "conditions",
        split(regexp_replace(concat_ws("|", col("clndn")), "_", " "), "\\|")
      )
      .withColumn("clndisdbincl", split(concat_ws("", col("clndisdbincl")), "\\|"))
      .withColumn("clndnincl", split(concat_ws("", col("clndnincl")), "\\|"))
      .withColumn("mc", split(concat_ws("", col("mc")), "\\|"))
      .withColumn("inheritance", inheritance_udf(col("origin")))
      .drop("clin_sig_original", "clndn")

  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(data.coalesce(1))
  }

  def inheritance_udf: UserDefinedFunction = udf { array: mutable.WrappedArray[String] =>
    val unknown = "unknown"

    val labels = Map(
      0          -> unknown,
      1          -> "germline",
      2          -> "somatic",
      4          -> "inherited",
      8          -> "paternal",
      16         -> "maternal",
      32         -> "de-novo",
      64         -> "biparental",
      128        -> "uniparental",
      256        -> "not-tested",
      512        -> "tested-inconclusive",
      1073741824 -> "other"
    )

    array match {
      //in case the inpout column is null return an Array("unknown")
      case null =>
        val wa: mutable.WrappedArray[String] = mutable.WrappedArray.make(Array(unknown))
        wa
      //in any other case transform the array of number into their labels
      case _ =>
        array
          .map(_.toInt)
          .flatMap { number =>
            labels.collect {
              case (_, _) if number == 0                 => unknown
              case (digit, str) if (number & digit) != 0 => str
            }
          }
          .distinct
    }
  }

  implicit class DataFrameOps(df: DataFrame) {
    def withInterpretations: DataFrame = {

      val fieldsToRemove: Seq[String] =
        Seq("chromosome", "start", "end", "reference", "alternate", "interpretation")

      val fieldsTokeep: Seq[String] = df.columns.filterNot(c => fieldsToRemove.contains(c))
      df
        .withColumn(
          "interpretations",
          array_remove(array_union(col("clin_sig"), col("clin_sig_conflict")), "")
        )
        .withColumn("interpretation", explode(col("interpretations")))
        .drop("interpretations")
        .groupBy("chromosome", "start", "end", "reference", "alternate")
        .agg(
          collect_set(col("interpretation")) as "interpretations",
          fieldsTokeep.map(c => first(c) as c): _*
        )
    }
  }
}
