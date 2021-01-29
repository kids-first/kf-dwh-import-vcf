package org.kidsfirstdrc.dwh.testutils

import org.apache.spark.sql.functions._
import org.kidsfirstdrc.dwh.external.ImportClinVar

object ClassGeneratorMain extends App with WithSparkSession {

  import spark.implicits._

  val root = "src/test/scala/"

  val clinvarPath = getClass.getResource("/input_vcf/clinvar.vcf").getFile
  val clinvarInput = spark.read.format("vcf").load(clinvarPath)
    .where($"contigName" === "2" and $"start" === 69359260 and $"end" === 69359261)
    .withColumn("sampleId", lit("id"))
    .withColumn("genotypes", array(struct(col("sampleId") as "sampleId")))
    .drop("sampleId")
  val clinvarOutput = ImportClinVar.transform(clinvarInput)


  ClassGenerator.writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","ClinvarInput", clinvarInput, root)
  ClassGenerator.writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","ClinvarOutput", clinvarOutput, root)

  val variants = spark.read.format("parquet").load("src/test/resources/variants/variants.parquet")

  ClassGenerator.writeCLassFile("org.kidsfirstdrc.dwh.testutils.variant","Variant", variants, root)
}
