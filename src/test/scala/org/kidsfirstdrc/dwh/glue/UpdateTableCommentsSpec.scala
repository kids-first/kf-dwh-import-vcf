package org.kidsfirstdrc.dwh.glue

import org.apache.spark.sql.functions.col
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UpdateTableCommentsSpec
    extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  val database = "variant"
  val table    = "orphanet_gene_set"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS ${database}")
  spark.sql(s"DROP TABLE IF EXISTS ${database}.${table}")
  spark.sql(s"""CREATE TABLE IF NOT EXISTS ${database}.${table}
       |(
       |orpha_code BIGINT COMMENT 'test',
       |expert_link STRING,
       |name STRING,
       |gene_synonym_list ARRAY<STRING>
       |)
       |""".stripMargin)

  "clearComments" should "remove all comments" in {
    UpdateTableComments.clearComments(database, table)
    val describeTableDF = spark.sql(s"DESCRIBE $database.$table")
    describeTableDF.show(false)
    describeTableDF.where(col("comment") =!= "").count() shouldBe 0
  }

  "run" should "read metadata from a json file and update comments" in {
    import spark.implicits._

    val metadata_file = this.getClass.getResource("/documentation/orphanet_gene_set.json").getFile

    spark.sql(s"DESCRIBE $database.$table").show(false)

    UpdateTableComments.run(database, table, metadata_file)

    val describeTableDF = spark.sql(s"DESCRIBE $database.$table")

    describeTableDF.show(false)

    describeTableDF.as[GlueFieldComment].collect() should contain allElementsOf Array(
      GlueFieldComment("orpha_code", "bigint", "orpha_code description"),
      GlueFieldComment("expert_link", "string", "expert_link description"),
      GlueFieldComment("name", "string", "name description"),
      GlueFieldComment("gene_synonym_list", "array<string>", "gene_synonym_list description")
    )
  }
}
