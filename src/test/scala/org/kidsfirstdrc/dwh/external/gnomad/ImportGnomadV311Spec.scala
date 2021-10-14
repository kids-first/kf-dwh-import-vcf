package org.kidsfirstdrc.dwh.external.gnomad

import bio.ferlab.datalake.commons.config.{Configuration, StorageConf}
import org.kidsfirstdrc.dwh.conf.Catalog.Raw
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.{GnomadV311Input, GnomadV311Output}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ImportGnomadV311Spec
    extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf("kf-strides-variant", getClass.getClassLoader.getResource(".").getFile))
    )

  "transform" should "transform genomes from gnomad" in {
    val inputData      = Map(Raw.gnomad_genomes_3_1_1_vcf.id -> Seq(GnomadV311Input()).toDF())
    val resultDF       = new ImportGnomadV311Job().transform(inputData)
    val expectedResult = GnomadV311Output()
    resultDF.as[GnomadV311Output].collect().head shouldBe expectedResult
  }

}
