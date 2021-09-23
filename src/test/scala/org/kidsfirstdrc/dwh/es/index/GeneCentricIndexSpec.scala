package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.spark3.config.Configuration
import org.apache.spark.sql.DataFrame
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.es.GeneCentricOutput
import org.kidsfirstdrc.dwh.testutils.external.GenesOutput
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GeneCentricIndexSpec
    extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  val genesDf: DataFrame = Seq(
    GenesOutput()
  ).toDF()

  val data = Map(
    Public.genes.id -> genesDf
  )

  implicit val conf: Configuration = Configuration(List())

  "Gene_centric index job" should "transform data to the right format" in {

    val result = new GeneCentricIndex("").transform(data)
    result.columns should contain allElementsOf Seq("hash")
    result.as[GeneCentricOutput].collect() should contain allElementsOf Seq(GeneCentricOutput())
  }
}
