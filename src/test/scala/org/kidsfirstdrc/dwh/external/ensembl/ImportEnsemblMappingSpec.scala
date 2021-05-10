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
        "kf-strides-variant", getClass().getClassLoader.getResource(".").getFile)))


  "run" should "creates ensembl_mapping table" in {

    val inputData = Map(
      Raw.ensembl_ena -> Seq(
        EnsemblEnaInput(),
        EnsemblEnaInput("Homo_sapiens","9606","ENSG00000284662","ENST00000332831","ENSP00000329982","chr1","ALI87340")).toDF(),
      Raw.ensembl_entrez -> Seq(EnsemblEntrezInput()).toDF(),
      Raw.ensembl_uniprot -> Seq(EnsemblUniprotInput()).toDF(),
      Raw.ensembl_refseq -> Seq(
        EnsemblRefseqInput(),
        EnsemblRefseqInput("ENSG00000284662","ENST00000332831","ENSP00000329982","NM_001005277","RefSeq_mRNA","DIRECT","94","100","-")).toDF(),
      Raw.ensembl_canonical -> Seq(
        EnsemblCanonicalInput(`_c2` = "Ensembl Canonical"),
        EnsemblCanonicalInput(`_c2` = "MANE Select v0.93"),
        EnsemblCanonicalInput(`_c2` = "MANE Plus Clinical v0.93")
      ).toDF()
    )

    val resultDF = new ImportEnsemblMapping().transform(inputData)
    resultDF.show(false)
    val expectedResult = EnsemblMappingOutput()
    resultDF.as[EnsemblMappingOutput].collect().head shouldBe expectedResult

  }

}

