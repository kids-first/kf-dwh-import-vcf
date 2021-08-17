package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.spark3.config.Configuration
import org.apache.spark.sql.DataFrame
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.testutils._
import org.kidsfirstdrc.dwh.testutils.es.{GenesSuggestOutput, SUGGEST}
import org.kidsfirstdrc.dwh.testutils.external.GenesOutput
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GenesSuggestionsIndexSpec
  extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  val genesDf: DataFrame = Seq(
    GenesOutput(
      `alias` = List("BII", "CACH6", "CACNL1A6", "Cav2.3", "", null)
    )
  ).toDF()

  val genesWithNullsDf: DataFrame = Seq(
    GenesOutput(
      `alias` = List("BII", "CACH6", "CACNL1A6", "Cav2.3", "", null),
      `ensembl_gene_id` = null
    )
  ).toDF()

  val data = Map(
    Public.genes.id          -> genesDf
  )

  implicit val conf: Configuration = Configuration(List())

  "suggester from genes" should "remove null and empty values" in {

    val result = new GenesSuggestionsIndex("").getGenesSuggest(genesWithNullsDf)
    result.show(false)

    val expectedResult = GenesSuggestOutput(
      `type` = "gene",
      `suggestion_id` = "9b8016c31b93a7504a8314ce3d060792f67ca2ad",
      `symbol` = "OR4F5",
      `ensembl_gene_id` = "",
      `suggest` =
        List(SUGGEST(List("OR4F5"), 5), SUGGEST(List("BII", "CACH6", "CACNL1A6", "Cav2.3"), 3))
    )

    result.as[GenesSuggestOutput].collect() should contain allElementsOf Seq(
      expectedResult
    )
  }
}
