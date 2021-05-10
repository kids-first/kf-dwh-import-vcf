package org.kidsfirstdrc.dwh.external.ensembl

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import org.kidsfirstdrc.dwh.conf.Catalog.Raw
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external._
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportEnsemblMappingSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf(
        "kf-strides-variant",
        getClass.getClassLoader.getResource(".").getFile)))


  "run" should "creates ensembl_mapping table" in {

    val inputData = Map(
      Raw.ensembl_ena -> Seq(EnsemblEnaInput()).toDF(),
      Raw.ensembl_entrez -> Seq(EnsemblEntrezInput()).toDF(),
      Raw.ensembl_uniprot -> Seq(EnsemblUniprotInput()).toDF(),
      Raw.ensembl_refseq -> Seq(EnsemblRefseqInput()).toDF(),
      Raw.ensembl_canonical -> Seq(EnsemblCanonicalInput()).toDF()
    )

    val resultDF = new ImportEnsemblMapping().transform(inputData)
    resultDF.show(false)
    val expectedResult = EnsemblMappingOutput()
    resultDF.as[EnsemblMappingOutput].collect().head shouldBe expectedResult

  }

}

