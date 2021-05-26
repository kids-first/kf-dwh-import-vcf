package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.spark3.config.{Configuration, StorageConf}
import org.kidsfirstdrc.dwh.conf.Catalog.Raw._
import org.kidsfirstdrc.dwh.external.orphanet.ImportOrphanetGeneSet
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external._
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportOrphanetProductSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf(
        "kf-strides-variant",
        getClass.getClassLoader.getResource(".").getFile)))

  "extract" should "return xml files parsed into a dataframes" in {
    import spark.implicits._

    val extractedData = new ImportOrphanetGeneSet().extract()
    val gene_associationDF = extractedData(orphanet_gene_association.id)
    val disease_historyDF = extractedData(orphanet_disease_history.id)

    val expectedProduct6 = OrphanetProduct6()
    gene_associationDF.show(false)
    gene_associationDF.count shouldBe 3
    gene_associationDF.where($"orpha_code" === 447).as[OrphanetProduct6].collect().head shouldBe expectedProduct6

    val expectedProduct9 = OrphanetProduct9()
    disease_historyDF.show(false)
    disease_historyDF.count shouldBe 2
    disease_historyDF.where($"orpha_code" === 58).as[OrphanetProduct9].collect().head shouldBe expectedProduct9
  }

  "transform" should "return a joined data from product 6 and 9 files" in {

    import spark.implicits._

    val job = new ImportOrphanetGeneSet()

    val extractedData = job.extract()
    val outputDF = job.transform(extractedData)

    outputDF.show(false)
    outputDF.where($"orpha_code" === 166024).as[OrphanetOutput].collect().head shouldBe OrphanetOutput()

    outputDF.as[OrphanetOutput].count() shouldBe 3

  }

  "load" should "create a hive table" in {

    import spark.implicits._

    spark.sql("CREATE DATABASE IF NOT EXISTS variant")

    val job = new ImportOrphanetGeneSet()

    job.run()
    val resultDF = spark.table(s"${job.destination.table.get.fullName}")

    resultDF.show(false)
    resultDF.where($"orpha_code" === 166024).as[OrphanetOutput].collect().head shouldBe OrphanetOutput()

    resultDF.as[OrphanetOutput].count() shouldBe 3

  }

}
