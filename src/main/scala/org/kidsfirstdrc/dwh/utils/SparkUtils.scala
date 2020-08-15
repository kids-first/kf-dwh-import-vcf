package org.kidsfirstdrc.dwh.utils

import io.projectglow.Glow
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DecimalType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object SparkUtils {
  def vcf(input: String)(implicit spark: SparkSession): DataFrame = {
    val inputs = input.split(",")
    val df = spark.read.format("vcf")
      .option("flattenInfoFields", "true")
      .load(inputs: _*)

    Glow.transform("split_multiallelics", df)
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
    val start: Column = (col("start") + 1) as "start"
    val end: Column = (col("end") + 1) as "end"

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
    def annotations(df: DataFrame): Column = {
      val colName = if (df.columns.contains("INFO_ANN")) "INFO_ANN" else "INFO_CSQ"
      when(col("splitFromMultiAllelic"), expr(s"filter($colName, ann-> ann.Allele == alternateAlleles[0])")).otherwise(col(colName)) as "annotations"
    }

    def firstAnn(df: DataFrame): Column = annotations(df).getItem(0) as "annotation"

    val consequences: Column = col("annotation.Consequence") as "consequences"
    val impact: Column = col("annotation.IMPACT") as "impact"
    val symbol: Column = col("annotation.SYMBOL") as "symbol"
    val feature_type: Column = col("annotation.Feature_type") as "feature_type"
    val ensembl_gene_id: Column = col("annotation.Gene") as "ensembl_gene_id"
    val ensembl_transcript_id: Column = when(col("annotation.Feature_type") === "Transcript", col("annotation.Feature")).otherwise(null) as "ensembl_transcript_id"
    val ensembl_regulatory_id: Column = when(col("annotation.Feature_type") === "RegulatoryFeature", col("annotation.Feature")).otherwise(null) as "ensembl_regulatory_id"
    val exon: Column = col("annotation.EXON") as "exon"
    val biotype: Column = col("annotation.BIOTYPE") as "biotype"
    val intron: Column = col("annotation.INTRON") as "intron"
    val hgvsc: Column = col("annotation.HGVSc") as "hgvsc"
    val hgvsp: Column = col("annotation.HGVSp") as "hgvsp"

    val strand: Column = col("annotation.STRAND") as "strand"
    val cds_position: Column = col("annotation.CDS_position") as "cds_position"
    val cdna_position: Column = col("annotation.cDNA_position") as "cdna_position"
    val protein_position: Column = col("annotation.Protein_position") as "protein_position"
    val amino_acids: Column = col("annotation.Amino_acids") as "amino_acids"
    val codons: Column = col("annotation.Codons") as "codons"
    val variant_class: Column = col("annotation.VARIANT_CLASS") as "variant_class"

    def hgvsg(df: DataFrame): Column = {
      val index = df.schema.fieldIndex("annotation")
      val isHGVSgpresent = df.schema(index).dataType.asInstanceOf[StructType]
        .fieldNames.contains("HGVSg")
      if (isHGVSgpresent) col("annotation.HGVSg") as "hgvsg" else lit(null).cast(StringType)
    }

    val is_multi_allelic: Column = col("splitFromMultiAllelic") as "is_multi_alellic"
    val old_multi_allelic: Column = col("INFO_OLD_MULTIALLELIC") as "old_multi_allelic"


    val locus: Seq[Column] = Seq(
      col("chromosome"),
      col("start"),
      col("end"),
      col("reference"),
      col("alternate"))
  }

}
