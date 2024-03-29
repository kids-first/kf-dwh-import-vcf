package org.kidsfirstdrc.dwh.update

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.kidsfirstdrc.dwh.conf.Catalog
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public}
import org.kidsfirstdrc.dwh.external.clinvar.ImportClinVarJob
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.{ClinvarOutput, GnomadV311Output, ImportScores, TopmedBravoOutput}
import org.kidsfirstdrc.dwh.testutils.join.{Freq, GnomadFreq, JoinConsequenceOutput}
import org.kidsfirstdrc.dwh.testutils.update.Variant
import org.kidsfirstdrc.dwh.updates.UpdateClinical
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, GivenWhenThen}

import scala.util.Try

class UpdateClinicalSpec
    extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers with BeforeAndAfter {
  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf(Catalog.kfStridesVariantBucket, getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  before {
    Try(spark.sql("DROP TABLE IF EXISTS variant.variants"))
    Try(spark.sql("DROP VIEW IF EXISTS variant.variants"))
    spark.sql("CREATE DATABASE IF NOT EXISTS variant_live")
    spark.sql("CREATE DATABASE IF NOT EXISTS variant")
  }

  "transform method for clinvar" should "return expected data given controlled input" in {

    val variant = Variant()
    val clinvar = ClinvarOutput()

    val variantDF = Seq(variant).toDF()
    val clinvarDF = Seq(clinvar).toDF()
    val data      = Map(Clinical.variants.id -> variantDF, Public.clinvar.id -> clinvarDF)

    val job      = new UpdateClinical(Public.clinvar, Clinical.variants, "variant")
    val resultDF = job.transform(data)

    val expectedResult = variant.copy(clinvar_id = Some(clinvar.name), clin_sig = clinvar.clin_sig)

    // Checks the input values were not the same before the join
    variant.clinvar_id should not be Some(clinvar.name)
    variant.clin_sig should not be clinvar.clin_sig
    // Checks the output values are the same as expected
    resultDF.as[Variant].collect().head shouldBe expectedResult
  }

  "transform method for topmed" should "return expected data given controlled input" in {

    val variant = Variant()
    val topmed  = TopmedBravoOutput()

    val variantDF = Seq(variant).toDF()
    val topmedDF  = Seq(topmed).toDF()
    val data      = Map(Clinical.variants.id -> variantDF, Public.topmed_bravo.id -> topmedDF)

    val job      = new UpdateClinical(Public.topmed_bravo, Clinical.variants, "variant")
    val resultDF = job.transform(data)

    val expectedResult = variant.copy(topmed = Some(Freq(10, 5, 0.5, 5, 0)))

    // Checks the input values were not the same before the join
    variant.topmed should not be Some(Freq(10, 5, 0.5, 5, 0))
    // Checks the output values are the same as expected
    resultDF.as[Variant].collect().head shouldBe expectedResult
  }

  "transform method for gnomad 3.1.1" should "return expected data" in {

    val variant   = Variant(start = 165310406, reference = "G", name = "rs1057520413")
    val gnomad311 = GnomadV311Output(an = 21, ac = 11, af = 0.51, nhomalt = 11)

    // Variant has an old gnomad_genomes_3_0 column.
    val variantDF   = Seq(variant).toDF()
    val gnomad311DF = Seq(gnomad311).toDF()
    val data        = Map(Clinical.variants.id -> variantDF, Public.gnomad_genomes_3_1_1.id -> gnomad311DF)

    val job      = new UpdateClinical(Public.gnomad_genomes_3_1_1, Clinical.variants, "variant")
    val resultDF = job.transform(data)

    val expectedResult =
      variant.copy(gnomad_genomes_3_1_1 = Some(GnomadFreq(an = 21, ac = 11, af = 0.51, hom = 11)))

    // Checks the input values were not the same before the join
    variant.gnomad_genomes_3_1_1 should not be Some(
      GnomadFreq(an = 21, ac = 11, af = 0.51, hom = 11)
    )

    // Checks the output values are the same as expected
    resultDF.as[Variant].collect().head shouldBe expectedResult
  }

  "transform method for dbnsfp_original" should "return expected data" in {

    val consequences = JoinConsequenceOutput(start = 165310406, reference = "G", alternate = "A", ensembl_transcript_id = "ENST00000486878", SIFT_converted_rankscore = None)
    val dbnsfp_original = ImportScores.Output(start = 165310406, reference = "G", `alternate` = "A", ensembl_transcript_id = "ENST00000486878", SIFT_converted_rankscore = Some(0.9))

    val consequencesDF = Seq(consequences).toDF()
    val dbnsfp_originalDF = Seq(dbnsfp_original).toDF()
    val data = Map(Clinical.consequences.id -> consequencesDF, Public.dbnsfp_original.id -> dbnsfp_originalDF)

    val job = new UpdateClinical(Public.dbnsfp_original, Clinical.consequences, "variant")
    val resultDF = job.transform(data)

    //checks that the result is still in the same format ie. JoinConsequenceOutput
    //also checks that SIFT_converted_rankscore is now the value of dbnsfp_original.SIFT_converted_rankscore
    resultDF.as[JoinConsequenceOutput].collect().head.`SIFT_converted_rankscore` shouldBe dbnsfp_original.`SIFT_converted_rankscore`
  }

  "load method" should "overwrite data" in {

    val variant = Variant()
    val clinvar = ClinvarOutput()

    val variantDF = Seq(variant).toDF()
    val clinvarDF = Seq(clinvar).toDF()

    Given("existing data")

    val job = new UpdateClinical(Public.clinvar, Clinical.variants, "variant")

    new ImportClinVarJob().load(clinvarDF)
    job.load(variantDF)

    val data = job.extract()
    data(Clinical.variants.id).show(false)
    data(Public.clinvar.id).show(false)

    val expectedResult = variant.copy(clinvar_id = Some(clinvar.name), clin_sig = clinvar.clin_sig)

    //runs the whole ETL job locally

    val resultJobDF = job.run()

    // Checks the job returned the same data as written on disk
    resultJobDF.as[Variant].collect().head shouldBe expectedResult

    // Checks the values on disk are the same as after the whole ETL was ran
    val resultDF = job.extract()(spark)(Clinical.variants.id)
    resultDF.as[Variant].collect().head shouldBe expectedResult

    ////checks the hive table was published and up to date
    val variantsHiveTable = spark.table(s"${Clinical.variants.table.get.fullName}")
    variantsHiveTable.as[Variant].collect().head shouldBe expectedResult
  }

}
