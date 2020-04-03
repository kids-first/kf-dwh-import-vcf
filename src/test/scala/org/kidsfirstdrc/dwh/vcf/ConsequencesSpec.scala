package org.kidsfirstdrc.dwh.vcf

import org.kidsfirstdrc.dwh.testutils.Model._
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ConsequencesSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {


  import spark.implicits._

  val studyId = "SD_123456"
  val releaseId = "RE_ABCDEF"
  "build" should "return a dataframe with all expected columns" in {
    val df = Seq(
      AnnotationInput("chr1", 1000, 1001, "C", Seq("G"), Nil, 0, Seq("mutation_1"),
        Seq(
          AnnInput("G", Seq("intron_variant", "non_coding_transcript_variant"), "MODIFIER", "WASH7P", "ENSG00000227232", "ENST00000488147", -1, "SNV", "chr1:g.1000C>G", Some(150), None, None),
          AnnInput("G", Seq("intron_variant", "non_coding_transcript_variant"), "MODIFIER", "WASH7P", "ENSG00000227232", "ENST00000488148", -1, "SNV", "chr1:g.1000C>G", Some(150), None, None)
        ), Nil
      ),
      AnnotationInput("chr1", 1000, 1001, "C", Seq("G"), Nil, 0, Seq("mutation_1"),
        Seq(
          AnnInput("G", Seq("intron_variant", "non_coding_transcript_variant"), "MODIFIER", "WASH7P", "ENSG00000227232", "ENST00000488147", -1, "SNV", "chr1:g.1000C>G", Some(150), None, None),
          AnnInput("G", Seq("intron_variant", "non_coding_transcript_variant"), "MODIFIER", "WASH7P", "ENSG00000227232", "ENST00000488148", -1, "SNV", "chr1:g.1000C>G", Some(150), None, None)
        ), Nil),
      AnnotationInput("chr2", 2000, 2001, "A", Seq("T"), Nil, 0, Nil, Seq(
        AnnInput("T", Seq("downstream_gene_variant"), "MODIFIER", "DDX11L1", "ENSG00000223972", "ENST00000450305", 1, "SNV", "chr2:g.2000A>T", Some(200), Some(AminoAcids("A", "T")), Some(25))),
        Nil)
    ).toDF()

    val output = Consequences.build(studyId, releaseId, df)
    output.as[ConsequenceOutput].collect() should contain theSameElementsAs Seq(
      ConsequenceOutput("1", 1000, 1001, "C", "G", "WASH7P", "MODIFIER", "ENSG00000227232", "intron_variant", -1, "chr1:g.1000C>G", Some("mutation_1"), "SNV", Seq("ENST00000488147", "ENST00000488148"), studyId, releaseId, Some(150), None, None),
      ConsequenceOutput("1", 1000, 1001, "C", "G", "WASH7P", "MODIFIER", "ENSG00000227232", "non_coding_transcript_variant", -1, "chr1:g.1000C>G", Some("mutation_1"), "SNV", Seq("ENST00000488147", "ENST00000488148"), studyId, releaseId, Some(150), None, None),
      ConsequenceOutput("2", 2000, 2001, "A", "T", "DDX11L1", "MODIFIER", "ENSG00000223972", "downstream_gene_variant", 1, "chr2:g.2000A>T", None, "SNV", Seq("ENST00000450305"), studyId, releaseId, Some(200), Some(AminoAcids("A", "T")), Some(25))
    )
  }

}



