package org.kidsfirstdrc.dwh.es.json

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.{DataSource, ETL}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.es.json.EsCatalog.{Clinical, Public}
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locus
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._

class SuggesterIndexJson(releaseId: String)(override implicit val conf: Configuration) extends ETL(EsCatalog.Es.suggester_index) {

  final val geneSymbolWeight = 5
  final val variantSymbolAaChangeWeight = 4
  final val variantSymbolWeight = 2

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(
      Public.genes -> spark.table(s"${Public.genes.database}.${Public.genes.name}"),
      Clinical.variants -> spark.read.parquet(s"${Clinical.variants.rootPath}/variants/variants_$releaseId"),
      Clinical.consequences -> spark.read.parquet(s"${Clinical.consequences.rootPath}/consequences/consequences_$releaseId")
    )
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val genes = data(Public.genes).select("symbol")
    val variants = data(Clinical.variants).selectLocus(col("hgvsg"))
    val consequences = data(Clinical.consequences)
      .selectLocus(col("symbol"), col("aa_change"))
      .dropDuplicates()

    getGenesSuggest(genes).unionByName(getVariantSuggest(variants, consequences))
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): Unit = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .format("json")
      .json(s"${destination.location}")
  }

  def getVariantSuggest(variants: DataFrame, consequence: DataFrame): DataFrame = {

    val groupedByLocusConsequences = consequence
      .withColumn("aa_change", when(col("aa_change").isNull, lit("")).otherwise(trim(col("aa_change"))))
      .withColumn("symbol", when(col("symbol").isNull, lit("")).otherwise(trim(col("symbol"))))
      .withColumn("symbol_aa_change", trim(concat_ws(" ", col("symbol"), col("aa_change"))))
      .groupBy(locus:_*)
      .agg(
        collect_set(col("symbol")) as "symbol",
        collect_set(col("aa_change")) as "aa_change",
        array_remove(collect_set(struct(
          col("symbol") as "symbol",
          col("aa_change") as "aa_change"
        )), struct(lit("") as "symbol", lit("") as "aa_change")) as "consequences" ,
        collect_set(col("symbol_aa_change")) as "symbol_aa_change"
      )

    variants
      .withColumn("hgvsg", when(col("hgvsg").isNull, lit("")).otherwise(trim(col("hgvsg"))))
      .joinByLocus(groupedByLocusConsequences, "left")
      .withColumn("type", lit("variant"))
      .withColumn("locus", concat_ws("-", locus:_*))
      .withColumn("suggestion_id", sha1(col("locus"))) //this maps to `hash` column in variant_centric index
      .withColumn("hgvsg", col("hgvsg"))
      .withColumn("suggest", array(
        struct(
          array_remove(flatten(array(col("symbol_aa_change"), array(col("hgvsg")), array(col("locus")))), "") as "input",
          lit(variantSymbolAaChangeWeight) as "weight"),
        struct(
          array_remove(col("symbol"), "") as "input",
          lit(variantSymbolWeight) as "weight"),
        ))
      .select("type", "locus", "suggestion_id", "hgvsg", "suggest", "consequences")
  }

  def getGenesSuggest(genes: DataFrame): DataFrame = {
    genes
      .withColumn("type", lit("gene"))
      .withColumn("suggestion_id", sha1(col("symbol"))) //this maps to `hash` column in gene_centric index
      .withColumn("hgvsg", lit(null).cast(StringType))
      .withColumn("locus", lit(null).cast(StringType))
      .withColumn("consequences", array())
      .withColumn("suggest", array(struct(
        array(col("symbol")) as "input",
        lit(geneSymbolWeight) as "weight"
      )))
      .select("type", "locus", "suggestion_id", "hgvsg", "suggest", "consequences")
  }
}
