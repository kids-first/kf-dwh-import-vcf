package org.kidsfirstdrc.dwh.testutils

import org.apache.spark.sql.functions._
import org.kidsfirstdrc.dwh.external.ImportClinVar

object ClassGeneratorMain extends App with WithSparkSession {

  import spark.implicits._

  val input = getClass.getResource("/input_vcf/clinvar.vcf").getFile

  val inputDF = spark.read.format("vcf").load(input)
    .where($"contigName" === "2" and $"start" === 69359260 and $"end" === 69359261)
    .withColumn("sampleId", lit("id"))
    .withColumn("genotypes", array(struct(col("sampleId") as "sampleId")))
    .drop("sampleId")

  val outputDf = ImportClinVar.transform(inputDF)

  val root = "src/test/scala/"
  ClassGenerator.writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","ClinvarInput", inputDF, root)
  ClassGenerator.writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","ClinvarOutput", outputDf, root)
}
