package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.kidsfirstdrc.dwh.testutils.Model._

class VariantsSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  val studyId = "SD_123456"
  val releaseId = "RE_ABCDEF"
  "build" should "return a dataframe with all expected columns" in {
    val df = Seq(
      VariantInput(INFO_AC = Seq(2), INFO_AN = 6, genotypes = Seq(hom_00, hom_11, het_01)),
      VariantInput(INFO_AC = Seq(4), INFO_AN = 6, genotypes = Seq(hom_00, hom_11, het_01)),
      VariantInput(contigName = "chr3", start = 2000, end = 2001, names = Seq("mutation_2"), referenceAllele = "A", alternateAlleles = Seq("T"), INFO_AC = Seq(4), INFO_AN = 6, INFO_ANN = Seq(ConsequenceInput(Allele = "A", HGVSg = "chr2:g.2000A>T")), genotypes = Seq(hom_00, het_10, hom_00))
    ).toDF()

    val output = Variants.build(studyId, releaseId, df)
    output.as[VariantOutput].collect() should contain theSameElementsAs Seq(
      VariantOutput(ac = 6, an = 12, af = 0.5, homozygotes = 2, heterozygotes = 2),
      VariantOutput(chromosome = "3", start = 2001, end = 2002, reference = "A", alternate = "T", name = Some("mutation_2"), hgvsg = "chr2:g.2000A>T", ac = 4, an = 6, af = 0.66666667, homozygotes = 0, heterozygotes = 1)
    )
  }

  it should "return a dataframe with all expected columns with multiallelic" in {
    val df = Seq(
      VariantInput(INFO_AC = Seq(2), INFO_AN = 6, genotypes = Seq(hom_00, hom_11, het_01), INFO_ANN = Seq(ConsequenceInput(), ConsequenceInput(Allele = "T", HGVSg = "chr2:g.166166916G>T")), splitFromMultiAllelic = true),
      VariantInput(alternateAlleles = Seq("T"), INFO_AC = Seq(4), INFO_AN = 6, genotypes = Seq(hom_00, hom_11, het_01), INFO_ANN = Seq(ConsequenceInput(), ConsequenceInput(Allele = "T", HGVSg = "chr2:g.166166916G>T")), splitFromMultiAllelic = true)
    ).toDF()

    val output = Variants.build(studyId, releaseId, df)
    output.as[VariantOutput].collect() should contain theSameElementsAs Seq(
      VariantOutput(ac = 2, an = 6, af = 0.33333333, homozygotes = 1, heterozygotes = 1),
      VariantOutput(alternate="T", hgvsg = "chr2:g.166166916G>T", ac = 4, an = 6, af = 0.66666667, homozygotes = 1, heterozygotes = 1)
    )
  }

  "genotype_states" should "run" in {

    import io.projectglow.Glow
    Glow.register(spark)
    val df = Seq(
      VariantInput(INFO_AC = Seq(2), INFO_AN = 6, genotypes = Seq(hom_00, hom_11, het_01), INFO_ANN = Seq(ConsequenceInput(), ConsequenceInput(Allele = "T", HGVSg = "chr2:g.166166916G>T")), splitFromMultiAllelic = true),
      VariantInput(alternateAlleles = Seq("T"), INFO_AC = Seq(4), INFO_AN = 6, genotypes = Seq(Genotype(Array(-1,1)), Genotype(Array(-1,0)), Genotype(Array(1,-1))), INFO_ANN = Seq(ConsequenceInput(), ConsequenceInput(Allele = "T", HGVSg = "chr2:g.166166916G>T")), splitFromMultiAllelic = true)
    ).toDF()


    val num_alt_alleles_df = df.selectExpr("genotype_states(genotypes) as num_alt_alleles_col")
    num_alt_alleles_df.show()

  }
}
