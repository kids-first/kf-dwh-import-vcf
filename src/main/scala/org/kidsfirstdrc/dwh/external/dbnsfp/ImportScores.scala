package org.kidsfirstdrc.dwh.external.dbnsfp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ImportScores extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import DBSNFP Scores to DWH").getOrCreate()

  import spark.implicits._

  import org.apache.spark.sql.Column
  import org.apache.spark.sql.types.DoubleType

  def split_semicolon(colName: String, outputColName: String): Column = split(col(colName), ";") as outputColName

  def split_semicolon(colName: String): Column = split_semicolon(colName, colName)

  def element_at_postion(colName: String) = element_at(col(colName), col("position")) as colName

  def score(colName: String) = when(element_at_postion(colName) === ".", null).otherwise(element_at_postion(colName).cast(DoubleType)) as colName

  def pred(colName: String) = when(element_at_postion(colName) === ".", null).otherwise(element_at_postion(colName)) as colName


  spark.table("variant.dbnsfp").select(
    $"chromosome",
    $"start",
    $"ref",
    $"alt",
    split_semicolon("aapos"),
    split_semicolon("refcodon"),
    split_semicolon("codonpos"),
    $"aaref",
    split_semicolon("Ensembl_proteinid", "ensembl_protein_id"),
    split_semicolon("genename", "symbol"),
    split_semicolon("Ensembl_geneid", "ensembl_gene_id"),
    posexplode(split($"Ensembl_transcriptid", ";")),
    split_semicolon("cds_strand"),
    split_semicolon("SIFT_score", "sift_score"),
    split_semicolon("SIFT_pred", "sift_pred"),
    $"SIFT_converted_rankscore" as "sift_converted_rank_score",
    split_semicolon("Polyphen2_HDIV_score", "polyphen2_hdiv_score"),
    split_semicolon("Polyphen2_HDIV_pred", "polyphen2_hdiv_pred"),
    $"Polyphen2_HDIV_rankscore" as "polyphen2_hdiv_rank_score",
    split_semicolon("Polyphen2_HVAR_score", "polyphen2_hvar_score"),
    split_semicolon("Polyphen2_HVAR_pred", "polyphen2_hvar_pred"),
    $"Polyphen2_HVAR_rankscore" as "polyphen2_hvar_rank_score",
    split_semicolon("FATHMM_score", "fathmm_score"),
    split_semicolon("FATHMM_pred", "fathmm_pred"),
    $"FATHMM_converted_rankscore" as "fathmm_converted_rank_score",
    $"REVEL_rankscore" as "revel_rankscore",
    $"LRT_converted_rankscore" as "lrt_converted_rankscore",
    $"LRT_pred" as "lrt_pred",
    $"CADD_raw" as "cadd_score",
    $"CADD_raw_rankscore" as "cadd_rankscore",
    $"CADD_phred" as "cadd_phred",
    $"DANN_score" as "dann_score",
    $"DANN_rankscore" as "dann_rank_score",
    $"phyloP100way_vertebrate" as "phylo_p100way_vertebrate",
    $"phyloP100way_vertebrate_rankscore" as "phylo_p100way_vertebrate_rankscore",
    $"phyloP30way_mammalian" as "phylop30way_mammalian",
    $"phyloP30way_mammalian_rankscore" as "phylo_p30way_mammalian_rankscore",
    $"phyloP17way_primate" as "phylop17way_primate",
    $"phyloP17way_primate_rankscore" as "phylo_p17way_primate_rankscore",
    $"phastCons100way_vertebrate" as "phast_cons100way_vertebrate",
    $"phastCons100way_vertebrate_rankscore" as "phast_cons100way_vertebrate_rankscore",
    $"phastCons30way_mammalian" as "phastcons30way_mammalian",
    $"phastCons30way_mammalian_rankscore" as "phast_cons30way_mammalian_rankscore",
    $"phastCons17way_primate" as "phast_cons17way_primate",
    $"phastCons17way_primate_rankscore" as "phast_cons17way_primate_rankscore",
    $"GERP++_NR" as "gerp_nr",
    $"GERP++_RS" as "gerp_rs",
    $"GERP++_RS_rankscore" as "gerp_rs_rankscore"
  )
    .withColumnRenamed("col", "ensembl_transcript_id")
    .withColumn("position", $"pos" + 1)
    .drop("pos")
    .select(
      $"chromosome",
      $"start",
      $"ref" as "reference",
      $"alt" as "alternate",
      $"aaref",
      element_at_postion("symbol"),
      element_at_postion("ensembl_gene_id"),
      element_at_postion("ensembl_protein_id"),
      $"ensembl_transcript_id",
      element_at_postion("cds_strand"),
      score("sift_score"),
      pred("sift_pred"),
      $"sift_converted_rank_score",

      score("polyphen2_hdiv_score"),
      pred("polyphen2_hdiv_pred"),
      $"polyphen2_hdiv_rank_score",

      score("polyphen2_hvar_score"),
      pred("polyphen2_hvar_pred"),
      $"polyphen2_hvar_rank_score",

      score("fathmm_score"),
      pred("fathmm_pred"),
      $"fathmm_converted_rank_score",

      $"cadd_score",
      $"cadd_rankscore",
      $"cadd_phred",
      $"dann_score",
      $"dann_rank_score",

      $"revel_rankscore",
      $"lrt_converted_rankscore",
      $"lrt_pred",

      $"phylo_p100way_vertebrate",
      $"phylo_p100way_vertebrate_rankscore",
      $"phylop30way_mammalian",
      $"phylo_p30way_mammalian_rankscore",
      $"phylop17way_primate",
      $"phylo_p17way_primate_rankscore",
      $"phast_cons100way_vertebrate",
      $"phast_cons100way_vertebrate_rankscore",
      $"phastcons30way_mammalian",
      $"phast_cons30way_mammalian_rankscore",
      $"phast_cons17way_primate",
      $"phast_cons17way_primate_rankscore",
      $"gerp_nr",
      $"gerp_rs",
      $"gerp_rs_rankscore"
    )
    .repartition($"chromosome")
    .sortWithinPartitions("start")
    .write.mode("overwrite")
    .partitionBy("chromosome")
    .format("parquet")
    .option("path", "s3a://kf-variant-parquet-prd/public/dbnsfp/parquet/scores")
    .saveAsTable("variant.dbnsfp_scores")


}

