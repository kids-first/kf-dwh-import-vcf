package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.commons.config.{Configuration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.kidsfirstdrc.dwh.conf.Catalog.Clinical
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.vcf.OccurrenceOutput
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class OccurrenceSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  val studyId      = "SD_123456"
  val releaseId    = "RE_ABCDEF"
  val releaseId_lc = releaseId.toLowerCase

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf("kf-strides-variant", getClass.getClassLoader.getResource(".").getFile, LOCAL))
    )

  "transform occurrences_family into occurrences" should "return a dataframe with all expected columns" in {

    val data = Map(
      Clinical.occurrences_family.id -> Seq(OccurrenceOutput()).toDF
    )

    spark.sql("use variant")

    val outputDf = new Occurrences(studyId, releaseId).transform(data)

    outputDf.as[OccurrenceOutput].count shouldBe 1

    outputDf.as[OccurrenceOutput].collect().head shouldBe OccurrenceOutput()

  }

}
