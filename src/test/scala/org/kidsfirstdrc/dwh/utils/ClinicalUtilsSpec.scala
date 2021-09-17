package org.kidsfirstdrc.dwh.utils

import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.utils.ClinicalUtils.getGenomicFiles
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ClinicalUtilsSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  val studyId   = "SD_123456"
  val releaseId = "RE_ABCDEF"

  "getGenomicFiles" should "return a dataframe with filenames and corresponding acls" in {
    withOutputFolder("tmp") { output =>
      spark.sql("CREATE DATABASE IF NOT EXISTS variant")

      val manifestDF = Seq(
        ("SD_123456", "file11"),
        ("SD_789", "file12")
      ).toDF("study_id", "file_name")

      manifestDF.write
        .mode(SaveMode.Overwrite)
        .option("path", s"$output/genomic_files_override")
        .format("json")
        .saveAsTable("variant.genomic_files_override")

      val df = Seq(
        (Seq("SD_123456", "SD_123456.c1"), "file1", "SD_123456", Array("s3://kf-study-us-east-1-prd-sd-123456/harmonized/family-variants/file1")),
        (Seq("SD_123456.c2", "SD_123456.c3"), "file6", "SD_123456", Array("s3://kf-study-us-east-1-prd-sd-123456/harmonized/family-variants/file6")),
        (Seq("SD_789.c1"), "file7", "SD_789", Array("s3://kf-study-us-east-1-prd-sd-789/harmonized/family-variants/file7"))
      ).toDF("acl", "file_name", "study_id", "urls")

      df.write
        .mode(SaveMode.Overwrite)
        .option("path", s"$output/genomic_files_re_abcdef")
        .format("json")
        .saveAsTable("variant.genomic_files_re_abcdef")

      val res = getGenomicFiles(studyId, releaseId)

      res should contain theSameElementsAs Seq(
        ("s3a://kf-study-us-east-1-prd-sd-123456/harmonized/family-variants/file11"),
        ("s3a://kf-study-us-east-1-prd-sd-123456/harmonized/family-variants/file1"),
        ("s3a://kf-study-us-east-1-prd-sd-123456/harmonized/family-variants/file6")
      )

    }
  }

}
