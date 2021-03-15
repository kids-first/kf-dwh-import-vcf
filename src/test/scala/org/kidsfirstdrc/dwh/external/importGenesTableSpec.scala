package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.conf.Environment
import org.kidsfirstdrc.dwh.testutils._
import org.kidsfirstdrc.dwh.testutils.external.{CosmicCancerGeneCensusOutput, DddGeneCensusOutput, OmimOutput, OrphanetOutput}
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
      Public.human_genes       -> Seq(HumanGenesOutput(), HumanGenesOutput(`symbol` = "OR4F4")).toDF(),
      Public.ddd_gene_set      -> Seq(DddGeneCensusOutput(`symbol` = "OR4F5")).toDF(),
      Public.cosmic_gene_set   -> Seq(CosmicCancerGeneCensusOutput(`symbol` = "OR4F5")).toDF
    )

    val resultDF = new ImportGenesTable(Environment.LOCAL).transform(inputData)

    val expectedOrphanet = List(ORPHANET(17601, "Multiple epiphyseal dysplasia, Al-Gazali type", List("Autosomal recessive")))
    val expectedOmim = List(OMIM("Shprintzen-Goldberg syndrome", "182212", List("AD")))

    resultDF.show(false)

    resultDF.where("symbol='OR4F5'").as[GenesOutput].collect().head shouldBe
      GenesOutput(`orphanet` = expectedOrphanet, `omim` = expectedOmim)

    resultDF
      .where("symbol='OR4F4'")
      .select(
        functions.size(col("orphanet")),
        functions.size(col("ddd")),
        functions.size(col("cosmic"))).as[(Long, Long, Long)].collect().head shouldBe (0, 0, 0)

  }

}
