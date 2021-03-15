package org.kidsfirstdrc.dwh.es.json

import bio.ferlab.datalake.core.config.Configuration
import org.apache.spark.sql.DataFrame
import org.kidsfirstdrc.dwh.es.json.EsCatalog.Public
import org.kidsfirstdrc.dwh.testutils.{GeneCentricOutput, GenesOutput, WithSparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class GeneCentricIndexJsonSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  val genesDf: DataFrame = Seq(
    GenesOutput()
  ).toDF()

  val data = Map(
    Public.genes -> genesDf
  )

  implicit val conf: Configuration =  Configuration(List())

  "Gene_centric index job" should "transform data to the right format" in {

    val result = new GeneCentricIndexJson().transform(data)
    result.columns should contain allElementsOf Seq("hash")
    result.as[GeneCentricOutput].collect() should contain allElementsOf Seq(GeneCentricOutput())
  }
}