package org.kidsfirstdrc.dwh.utils

import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.utils.ClinicalUtils.getGenomicFiles
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ClinicalUtilsSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {


  import spark.implicits._

  val studyId = "SD_123456"
  val releaseId = "RE_ABCDEF"

  "getGenomicFiles" should "return a dataframe with filenames and corresponding acls" in {
    withOutputFolder("tmp") { output =>

      spark.sql("CREATE DATABASE IF NOT EXISTS variant")

      val manifestDF = Seq(
        ("RE_ABCDEF", "file1")
      ).toDF("study_id", "file_name")

      manifestDF.write.mode(SaveMode.Overwrite)
        .option("path", s"$output/genomic_files_override")
        .format("json")
        .saveAsTable("variant.genomic_files_override")

      val df = Seq(
        (Seq("SD_123456", "SD_123456.c1"), "file1", "SD_123456"),
        (Seq("SD_123456.c2"), "file2", "SD_123456"),
        (Seq("*"), "file3", "SD_123456"),
        (null, "file4", "SD_123456"),
        (Seq("SD_123456"), "file5", "SD_123456"),
        (Seq("SD_123456.c2", "SD_123456.c3"), "file6", "SD_123456"),
        (Seq("SD_123456.c1"), "file7", "SD_789"),
        (Seq("aaaa.c999"), "file8", "SD_123456")
      ).toDF("acl", "file_name", "study_id")
      df.write.mode(SaveMode.Overwrite)
        .option("path", s"$output/genomic_files_re_abcdef")
        .format("json")
        .saveAsTable("genomic_files_re_abcdef")

      val res = getGenomicFiles(studyId, releaseId)

      res.show(false)

      res.as[String].collect() should contain theSameElementsAs Seq(
        ("file1"),
        ("file2"),
        ("file3"),
        ("file4"),
        ("file5"),
        ("file8"),
        ("file6")
      )

    }
  }



}



