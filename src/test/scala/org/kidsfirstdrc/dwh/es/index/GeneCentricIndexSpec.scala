package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.commons.config.Configuration
import org.apache.spark.sql.DataFrame
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.es.GeneCentricOutput
import org.kidsfirstdrc.dwh.testutils.external.GenesOutput
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GeneCentricIndexSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  def makeDataMap(g: GenesOutput): Map[String, DataFrame] = Map(
    Public.genes.id -> Seq(
      g
    ).toDF()
  )

  implicit val conf: Configuration = Configuration(List())

  "Gene_centric index job" should "transform data to the right format" in {

    val result = new GeneCentricIndex("").transform(makeDataMap(GenesOutput()))
    result.columns should contain allElementsOf Seq("hash")
    result.as[GeneCentricOutput].collect() should contain allElementsOf Seq(GeneCentricOutput())
  }

  "Gene_centric index job" should "create a search_text column" in {
    // all
    val g0 = GenesOutput(`symbol` = "s0", `alias` = List("a01"), `ensembl_gene_id` = "egi0")
    val r0 = new GeneCentricIndex("").transform(makeDataMap(g0))
    r0.columns should contain allElementsOf Seq("search_text")
    r0.select("search_text").as[List[String]].collect() should contain theSameElementsAs Seq(Seq("s0", "egi0", "a01"))

    // no ensembl_gene_id
    val g1 = GenesOutput(`symbol` = "s1", `alias` = List("a11", "a12"), `ensembl_gene_id` = null)
    val r1 = new GeneCentricIndex("").transform(makeDataMap(g1))
    r1.columns should contain allElementsOf Seq("search_text")
    r1.select("search_text").as[List[String]].collect() should contain theSameElementsAs Seq(Seq("s1", "a11", "a12"))

    // no alias
    val g2 = GenesOutput(`symbol` = "s2", `alias` = null, `ensembl_gene_id` = "egi2")
    val r2 = new GeneCentricIndex("").transform(makeDataMap(g2))
    r2.columns should contain allElementsOf Seq("search_text")
    r2.select("search_text").as[List[String]].collect() should contain theSameElementsAs Seq(Seq("s2", "egi2"))

    // edge case
    val g3 = GenesOutput(`symbol` = null, `alias` = null, `ensembl_gene_id` = null)
    val r3 = new GeneCentricIndex("").transform(makeDataMap(g3))
    r3.select("search_text").as[List[String]].collect() shouldBe Seq(Seq())
  }
}
