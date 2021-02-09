package org.kidsfirstdrc.dwh.testutils

import org.apache.spark.sql.functions._
import org.kidsfirstdrc.dwh.external.omim.ImportOmimGeneSet
import org.kidsfirstdrc.dwh.external.orphanet.ImportOrphanetJob
import org.kidsfirstdrc.dwh.testutils.ClassGeneratorImplicits._
import org.kidsfirstdrc.dwh.testutils.external.OmimInput
import org.kidsfirstdrc.dwh.utils.Catalog.Raw
import org.kidsfirstdrc.dwh.utils.Catalog.Raw._
import org.kidsfirstdrc.dwh.utils.Environment

object ClassGeneratorMain extends App with WithSparkSession {

  val root = "src/test/scala/"

  import spark.implicits._

  /** PREVENTS re-writting these classes by mistake
   *
  val clinvarPath = getClass.getResource("/input_vcf/clinvar.vcf").getFile
  val clinvarInput = spark.read.format("vcf").load(clinvarPath)
    .where($"contigName" === "2" and $"start" === 69359260 and $"end" === 69359261)
    .withColumn("sampleId", lit("id"))
    .withColumn("genotypes", array(struct(col("sampleId") as "sampleId")))
    .drop("sampleId")

  clinvarInput
    .writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","ClinvarInput", root)

  ImportClinVar
    .transform(clinvarInput)
    .writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","ClinvarOutput", root)


  spark.read.format("parquet").load("src/test/resources/variants/variants.parquet")
    .writeCLassFile("org.kidsfirstdrc.dwh.testutils.variant","Variant", root)


  val orphanetPath = getClass.getResource("/raw/orphanet").getFile
  val orphanetData = new ImportOrphanetJob(Environment.LOCAL).extract()(spark)
  orphanetData(orphanet_gene_association).where(col("orpha_code") === 447)
    .writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","OrphanetProduct6", root)

  orphanetData(orphanet_disease_history).where(col("orpha_code") === 58)
    .writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","OrphanetProduct9", root)

  new ImportOrphanetJob(Environment.LOCAL).transform(orphanetData)(spark)
    .where(col("orpha_code") === 166024)
    .writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","OrphanetOutput", root)
   */

  val omimInput = new ImportOmimGeneSet(Environment.LOCAL)
    .extract()
  omimInput(Raw.omim_genemap2)
    .where("_c0='chr1' and _c1=2228318 and _c2=2310212")
    .writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","OmimInput", root)

  new ImportOmimGeneSet(Environment.LOCAL).transform(Map(omim_genemap2 -> Seq(OmimInput()).toDF))
    .writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","OmimOutput", root)

}
