package org.kidsfirstdrc.dwh.external.omim

import org.kidsfirstdrc.dwh.external.omim.OmimPhenotype.parse_pheno
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class OmimPhenotypeSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  "parse_pheno" should "return phenotypes parsed as a structure" in {
    val df = Seq(
      "[Bone mineral density QTL 3], 606928 (2)",
      " {Epilepsy, idiopathic generalized, 10}, 613060 (2), Autosomal dominant",
      "Myasthenic syndrome, congenital, 8, with pre- and postsynaptic defects, 615120 (3), Autosomal recessive, X-linked",
      null,
      "unmatch"
    ).toDF("pheno")

    val frame = df.select(parse_pheno($"pheno") as "p")
    frame.where($"p".isNull).count() shouldBe 2

    frame.where($"p".isNotNull).select("p.*").as[OmimPhenotype].collect() should contain theSameElementsAs Seq(
      OmimPhenotype("[Bone mineral density QTL 3]", "606928", None, None),
      OmimPhenotype("Epilepsy, idiopathic generalized, 10", "613060", Some(Array("Autosomal dominant")), Some(Array("AD"))),
      OmimPhenotype("Myasthenic syndrome, congenital, 8, with pre- and postsynaptic defects", "615120", Some(Array("Autosomal recessive", "X-linked")), Some(Array("AR", "XL")))
    )

  }

}
