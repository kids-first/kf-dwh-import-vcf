package org.kidsfirstdrc.dwh.vcf

import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.kidsfirstdrc.dwh.testutils.Model._

class VariantsSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  def ann(allele: String, variant_class: String, hgvsg: String) = AnnInput(allele, Nil, "", "", "", "", 1, variant_class, hgvsg, None, None, None)

  import spark.implicits._

  val studyId = "SD_123456"
  val releaseId = "RE_ABCDEF"
  "build" should "return a dataframe with all expected columns" in {
    val df = Seq(
      AnnotationInput("chr1", 1000, 1001, "C", Seq("G"), Seq(2), 6, Seq("mutation_1"), Seq(ann("-", "SNV", "chr1:g.1000C>G")), Seq(hom_00, hom_11, het_01)),
      AnnotationInput("chr1", 1000, 1001, "C", Seq("G"), Seq(4), 6, Seq("mutation_1"), Seq(ann("-", "SNV", "chr1:g.1000C>G")), Seq(hom_00, hom_11, het_01)),
      AnnotationInput("chr2", 2000, 2001, "A", Seq("T"), Seq(4), 6, Nil, Seq(ann("-", "SNV", "chr2:g.2000A>T")), Seq(het_10, hom_00, hom_00))
    ).toDF()

    val output = Variants.build(studyId, releaseId, df)
    output.as[AnnotationOutput].collect() should contain theSameElementsAs Seq(
      AnnotationOutput("1", 1000, 1001, "C", "G", "chr1:g.1000C>G", Some("mutation_1"), 6, 12, 0.5, "SNV", 2, 2, studyId, releaseId),
      AnnotationOutput("2", 2000, 2001, "A", "T", "chr2:g.2000A>T", None, 4, 6, 0.66666667, "SNV", 0, 1, studyId, releaseId)
    )
  }

  it should "return a dataframe with all expected columns with multiallelic" in {
    val df = Seq(
      AnnotationInput("chr1", 1000, 1001, "C", Seq("G"), Seq(2), 6, Seq("mutation_1"), Seq(ann("G", "SNV", "chr1:g.1000C>G"), ann("T", "SNV", "chr1:g.1000C>T")), Seq(hom_00, hom_11, het_01), splitFromMultiAllelic = true),
      AnnotationInput("chr1", 1000, 1001, "C", Seq("T"), Seq(4), 6, Seq("mutation_1"), Seq(ann("G", "SNV", "chr1:g.1000C>G"), ann("T", "SNV", "chr1:g.1000C>T")), Seq(hom_00, hom_11, het_01), splitFromMultiAllelic = true)
    ).toDF()

    val output = Variants.build(studyId, releaseId, df)
    output.as[AnnotationOutput].collect() should contain theSameElementsAs Seq(
      AnnotationOutput("1", 1000, 1001, "C", "G", "chr1:g.1000C>G", Some("mutation_1"), 2, 6, 0.33333333, "SNV", 1, 1, studyId, releaseId),
      AnnotationOutput("1", 1000, 1001, "C", "T", "chr1:g.1000C>T", Some("mutation_1"), 4, 6, 0.66666667, "SNV", 1, 1, studyId, releaseId)
    )
  }

}


