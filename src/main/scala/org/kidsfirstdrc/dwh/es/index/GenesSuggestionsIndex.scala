package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.SparkUtils.getColumnOrElse
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Es, Public}

import java.time.LocalDateTime

class GenesSuggestionsIndex(releaseId: String)
                           (override implicit val conf: Configuration) extends ETL() {

  val destination: DatasetConf = Es.genes_suggestions

  final val geneSymbolWeight            = 5
  final val geneAliasesWeight           = 3

  final val indexColumns =
    List("type", "symbol", "suggestion_id", "suggest", "ensembl_gene_id")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      Public.genes.id -> spark.table(s"${Public.genes.table.get.fullName}"),
      Clinical.variants.id -> spark.read.parquet(
        s"${Clinical.variants.rootPath}/variants/variants_$releaseId"
      ),
      Clinical.consequences.id -> spark.read.parquet(
        s"${Clinical.consequences.rootPath}/consequences/consequences_$releaseId"
      )
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val genes = data(Public.genes.id).select("symbol", "alias", "ensembl_gene_id")

    getGenesSuggest(genes)
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    data.write
      .mode(SaveMode.Overwrite)
      .option("format", destination.format.sparkFormat)
      .option("path", s"${destination.location}_$releaseId")
      .saveAsTable(s"${destination.table.get.fullName}_${releaseId}")
    data
  }

  def getGenesSuggest(genes: DataFrame): DataFrame = {
    genes
      .withColumn("ensembl_gene_id", getColumnOrElse("ensembl_gene_id"))
      .withColumn("symbol", getColumnOrElse("symbol"))
      .withColumn("type", lit("gene"))
      .withColumn("suggestion_id", sha1(col("symbol"))) //this maps to `hash` column in gene_centric index
      .withColumn(
        "suggest",
        array(
          struct(
            array(col("symbol")) as "input",
            lit(geneSymbolWeight) as "weight"
          ),
          struct(
            array_remove(
              flatten(
                array(
                  functions.transform(col("alias"), c => when(c.isNull, lit("")).otherwise(c)),
                  array(col("ensembl_gene_id"))
                )
              ),
              ""
            ) as "input",
            lit(geneAliasesWeight) as "weight"
          )
        )
      )
      .select(indexColumns.head, indexColumns.tail: _*)
  }
}
