package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}
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

  def firstAs(c: String): Column = first(col(c)) as c

  object columns {
    val chromosome: Column = ltrim(col("contigName"), "chr") as "chromosome"
    val reference: Column = col("referenceAllele") as "reference"
    val alternate: Column = col("alternateAlleles")(0) as "alternate"
    val af: Column = col("ac").divide(col("an")).cast(DecimalType(8, 8)) as "af"
    val name: Column = col("names")(0) as "name"
    val ac: Column = col("INFO_AC")(0) as "ac"
    val an: Column = col("INFO_AN") as "an"
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
