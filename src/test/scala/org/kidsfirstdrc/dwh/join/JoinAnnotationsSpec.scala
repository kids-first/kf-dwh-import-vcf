package org.kidsfirstdrc.dwh.join

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class JoinAnnotationInput(chromosome: String, start: Long, end: Long, reference: String, alternate: String,
                               an: Long, ac: Long, af: BigDecimal, homozygotes: Long, heterozygotes: Long,
                               study_id: String, release_id: String,
                               name: String = "name", hgvsg: String = "hgvsg", variant_class: String = "variant_class")

case class JoinAnnotationOutput(chromosome: String, start: Long, end: Long, reference: String, alternate: String,
                                an: Long, ac: Long, af: BigDecimal, homozygotes: Long, heterozygotes: Long,
                                by_study: Map[String, Freq], release_id: String,
                                name: String = "name", hgvsg: String = "hgvsg", variant_class: String = "variant_class")

case class Freq(an: Long, ac: Long, af: BigDecimal, homozygotes: Long, heterozygotes: Long)

class JoinAnnotationsSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  val releaseId = "RE_ABCDEF"

  "build" should "return a dataframe with all expected columns" in {
    spark.sql("use variant")
    Given("2 studies")
    val (studyId1, studyId2) = ("SD_123", "SD_456")

    And("An empty output directory")
    val outputDir = "tmp/output_test"
    FileUtils.deleteDirectory(new File(outputDir))

    Given("2 tables, one  for each study")
    //Study 1
    val annotation1 = JoinAnnotationInput(chromosome = "1", start = 1000, end = 1000, "A", "T", 10, 5, 0.5, 2, 3, studyId1, releaseId)
    val annotation2 = JoinAnnotationInput(chromosome = "2", start = 2000, end = 2000, "T", "G", 20, 5, 0.25, 1, 4, studyId1, releaseId)

    Seq(annotation1, annotation2).toDF().write.mode(SaveMode.Overwrite)
      .option("path", s"$outputDir/annotations_sd_123_re_abcdef")
      .format("parquet")
      .saveAsTable("annotations_sd_123_re_abcdef")

    //Study 2
    val annotation3 = JoinAnnotationInput(chromosome = "3", start = 3000, end = 3000, "C", "A", 30, 10, 0.33333333, 2, 8, studyId2, releaseId)
    val annotation4 = annotation1.copy(study_id = studyId2)

    Seq(annotation3, annotation4).toDF().write.mode(SaveMode.Overwrite)
      .option("path", s"$outputDir/annotations_sd_456_re_abcdef")
      .format("parquet")
      .saveAsTable("annotations_sd_456_re_abcdef")

    Given("1 existing table annotation that contains some data for at least one study")
    val studyId3 = "SD_789"
    val annotation_output1 = JoinAnnotationOutput(chromosome = "1", start = 1000, end = 1000, "A", "T", 6, 4, 0.66666667, 2, 2, Map(
      studyId1 -> Freq(3, 2, 0.66666667, 1, 1),
      studyId3 -> Freq(7, 2, 0.5, 5, 1)
    ), "RE_PREVIOUS")

    val annotation_output2 = JoinAnnotationOutput(chromosome = "4", start = 4000, end = 4000, "T", "G", 3, 2, 0.66666667, 1, 1, Map(
      studyId3 -> Freq(3, 2, 0.66666667, 1, 1)
    ), "RE_PREVIOUS")

    Seq(annotation_output1, annotation_output2).toDF().write.mode(SaveMode.Overwrite)
      .option("path", s"$outputDir/annotations")
      .format("parquet")
      .saveAsTable("annotations")

    spark.table("annotations").printSchema()

    When("Join annotations")
    JoinAnnotations.join(Seq(studyId1, studyId2), releaseId, outputDir)

    Then("A new table for the release is created")
    val annotationReleaseTable = spark.table("variant.annotations_re_abcdef")

    And("this table should contain all merged data")
    val output = annotationReleaseTable
      .select("chromosome", "start", "end", "reference", "alternate", "an", "ac", "af",
        "homozygotes", "heterozygotes", "by_study", "release_id", "name", "hgvsg", "variant_class")
      .as[JoinAnnotationOutput]
    val expectedOutput = Seq(
      JoinAnnotationOutput(chromosome = "1", start = 1000, end = 1000, "A", "T", 27, 12, 0.44444444, 9, 7, Map(
        studyId1 -> Freq(10, 5, 0.5, 2, 3),
        studyId3 -> Freq(7, 2, 0.5, 5, 1),
        studyId2 -> Freq(10, 5, 0.5, 2, 3)
      ), releaseId),
      JoinAnnotationOutput(chromosome = "2", start = 2000, end = 2000, "T", "G", 20, 5, 0.25, 1, 4, Map(
        studyId1 -> Freq(20, 5, 0.25, 1, 4)
      ), releaseId),
      JoinAnnotationOutput(chromosome = "3", start = 3000, end = 3000, "C", "A", 30, 10, 0.33333333, 2, 8, Map(
        studyId2 -> Freq(30, 10, 0.33333333, 2, 8)
      ), releaseId),
      annotation_output2.copy(release_id = releaseId)
    )

    output.collect() should contain theSameElementsAs expectedOutput

  }

}


