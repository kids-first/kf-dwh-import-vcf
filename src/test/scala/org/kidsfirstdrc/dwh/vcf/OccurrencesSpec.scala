package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.testutils.Model.{ConsequenceInput, _}
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.vcf.Occurrences.filename
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class OccurrencesSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {


  import spark.implicits._

  val studyId = "SD_123456"
  val releaseId = "RE_ABCDEF"

  "getGenomicFiles" should "return a dataframe with filenames and corresponding acls" in {
    withOutputFolder("tmp") { output =>

      val df = Seq(
        (Seq("SD_123456", "SD_123456.c1"), "file1", "SD_123456"),
        (Seq("SD_123456.c2"), "file2", "SD_123456"),
        (Seq("*"), "file3", "SD_123456"),
        (null, "file4", "SD_123456"),
        (Seq("SD_123456"), "file5", "SD_123456"),
        (Seq("SD_123456.c2", "SD_123456.c3"), "file6", "SD_123456"),
        (Seq("SD_123456.c1"), "file7", "SD_789")
      ).toDF("acl", "file_name", "study_id")
      df.write.mode(SaveMode.Overwrite)
        .option("path", s"$output/genomic_files_re_abcdef")
        .format("json")
        .saveAsTable("genomic_files_re_abcdef")
      val res = Occurrences.getGenomicFiles(studyId, releaseId)
      res.as[(String, String)].collect() should contain theSameElementsAs Seq(
        ("SD_123456.c1", "file1"),
        ("SD_123456.c2", "file2"),
        ("_ALL_", "file3"),
        ("_NONE_", "file4"),
        ("_NONE_", "file5")
      )


    }
  }

  it should "return a dataframe with one ConsequenceInput with regulatory feature" in {
    val df = Seq(
      VariantInput(
        INFO_ANN = Seq(
          ConsequenceInput(Feature_type = "RegulatoryFeature", Feature = "ENSR0000636135")
        )
      )
    ).toDF()

    val output = Consequences.build(studyId, releaseId, df)
    output.as[ConsequenceOutput].collect() should contain theSameElementsAs Seq(
      ConsequenceOutput(ensembl_transcript_id = None, ensembl_regulatory_id = Some("ENSR0000636135"), feature_type = "RegulatoryFeature")
    )
  }

  "filename" should "return name of the input files" in {
    val df = spark.read.json(getClass.getResource("/filename").getFile).select($"id", filename)
    df.as[(String, String)].collect() should contain theSameElementsAs Seq(
      ("1", "file1.json"),
      ("2", "file1.json"),
      ("3", "file2.json")
    )
  }

}



