package org.kidsfirstdrc.dwh.external

import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.conf.Environment
import org.kidsfirstdrc.dwh.testutils.external.{OmimOutput, OrphanetOutput}
import org.kidsfirstdrc.dwh.testutils._
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class importGenesTableSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  "run" should "creates genes table" in {

    val inputData = Map(
      Public.omim_gene_set     -> Seq(OmimOutput(omim_gene_id = 601013)).toDF(),
      Public.orphanet_gene_set -> Seq(OrphanetOutput(gene_symbol = "OR4F5")).toDF(),
      Public.hpo_gene_set      -> Seq(HpoGeneSetOutput()).toDF(),
      Public.human_genes       -> Seq(HumanGenesOutput()).toDF()
    )

    val resultDF = new ImportGenesTable(Environment.LOCAL).transform(inputData)

    val expectedOrphanet = List(ORPHANET(17601, "Multiple epiphyseal dysplasia, Al-Gazali type", List("Autosomal recessive")))
    val expectedOmim = List(OMIM("Shprintzen-Goldberg syndrome", "182212", List("AD")))

    resultDF.as[GenesOutput].collect().head shouldBe GenesOutput(`orphanet` = expectedOrphanet, `omim` = expectedOmim)

  }

}
