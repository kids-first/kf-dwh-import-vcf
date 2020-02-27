package org.kidsfirstdrc.dwh.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DecimalType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object SparkUtils {
  def vcf(input: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.read.format("vcf")
      .option("flattenInfoFields", "true")
      .option("splitToBiallelic", "true")
      .load(input)
      .where($"splitFromMultiAllelic" === lit(false))
  }

  def tableName(table: String, studyId: String, releaseId: String): String = {
    s"${table}_${studyId.toLowerCase}_${releaseId.toLowerCase}"
  }

  def colFromArrayOrField(df: DataFrame, colName: String): Column = {
    df.schema(colName).dataType match {
      case ArrayType(_, _) => df(colName)(0)
      case _ => df(colName)
    }
  }

  def firstAs(c: String): Column = first(col(c)) as c

  object columns {
    val chromosome: Column = ltrim(col("contigName"), "chr") as "chromosome"
    val reference: Column = col("referenceAllele") as "reference"
    val alternate: Column = col("alternateAlleles")(0) as "alternate"
    val name: Column = col("names")(0) as "name"
    val calculated_af: Column = col("ac").divide(col("an")).cast(DecimalType(8, 8)) as "af"

    val ac: Column = col("INFO_AC")(0) as "ac"
    val af: Column = col("INFO_AF")(0) as "af"
    val an: Column = col("INFO_AN") as "an"

    val afr_af: Column = col("INFO_AFR_AF")(0) as "afr_af"
    val eur_af: Column = col("INFO_EUR_AF")(0) as "eur_af"
    val sas_af: Column = col("INFO_SAS_AF")(0) as "sas_af"
    val amr_af: Column = col("INFO_AMR_AF")(0) as "amr_af"
    val eas_af: Column = col("INFO_EAS_AF")(0) as "eas_af"

    val dp: Column = col("INFO_DP") as "dp"

    val countHomozygotesUDF: UserDefinedFunction = udf { calls: Seq[Seq[Int]] => calls.map(_.sum).count(_ == 2) }
    val homozygotes: Column = countHomozygotesUDF(col("genotypes.calls")) as "homozygotes"
    val countHeterozygotesUDF: UserDefinedFunction = udf { calls: Seq[Seq[Int]] => calls.map(_.sum).count(_ == 1) }
    val heterozygotes: Column = countHeterozygotesUDF(col("genotypes.calls")) as "heterozygotes"

    //Annotations
    val annotations: Column = col("INFO_ANN") as "annotations"
    val firstAnn: Column = split(col("INFO_ANN")(0), "\\|") as "annotation"
    val consequences: Column = col("annotation")(1) as "consequences"
    val impact: Column = col("annotation")(2) as "impact"
    val symbol: Column = col("annotation")(3) as "symbol"
    val gene_id: Column = col("annotation")(4) as "gene_id"
    val transcript: Column = col("annotation")(6) as "transcript"
    val strand: Column = col("annotation")(19).cast(IntegerType) as "strand"
    val variant_class: Column = col("annotation")(21) as "variant_class"
    val hgvsg: Column = col("annotation")(27) as "hgvsg"

    val locus: Seq[Column] = Seq(
      col("chromosome"),
      col("start"),
      col("end"),
      col("reference"),
      col("alternate"))
  }

}
