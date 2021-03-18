package org.kidsfirstdrc.dwh.external.dbnsfp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.conf.Ds
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.DsETL

class ImportScores(runEnv: Environment) extends DsETL(runEnv) {

  override val destination: Ds = Public.dbnsfp_original

  def split_semicolon(colName: String, outputColName: String): Column = split(col(colName), ";") as outputColName

  def split_semicolon(colName: String): Column = split_semicolon(colName, colName)

  def element_at_postion(colName: String): Column = element_at(col(colName), col("position")) as colName

  def score(colName: String): Column = when(element_at_postion(colName) === ".", null).otherwise(element_at_postion(colName).cast(DoubleType)) as colName

  def cast(colName: String): Column = col(colName).cast(DoubleType) as colName

  def pred(colName: String): Column = when(element_at_postion(colName) === ".", null).otherwise(element_at_postion(colName)) as colName

  override def extract()(implicit spark: SparkSession): Map[Ds, DataFrame] = {
    Map(
      Public.dbnsfp_variant -> spark.table(s"${Public.dbnsfp_variant.database}.${Public.dbnsfp_variant.name}")
    )
  }

  override def transform(data: Map[Ds, DataFrame])(implicit spark: SparkSession): DataFrame = {
    data(Public.dbnsfp_variant).select(
      col("chromosome"),
      col("start").cast(LongType),
      col("reference"),
      col("alternate"),
      split_semicolon("aapos"),
      split_semicolon("refcodon"),
      split_semicolon("codonpos"),
      col("aaref"),
      split_semicolon("Ensembl_proteinid", "ensembl_protein_id"),
      split_semicolon("genename", "symbol"),
      split_semicolon("Ensembl_geneid", "ensembl_gene_id"),
      split_semicolon("VEP_canonical"),
      posexplode(split(col("Ensembl_transcriptid"), ";")),
      split_semicolon("cds_strand"),
      split_semicolon("SIFT_score"),
      split_semicolon("SIFT_pred"),
      col("SIFT_converted_rankscore"),
      split_semicolon("Polyphen2_HDIV_score"),
      split_semicolon("Polyphen2_HDIV_pred"),
      col("Polyphen2_HDIV_rankscore"),
      split_semicolon("Polyphen2_HVAR_score"),
      split_semicolon("Polyphen2_HVAR_pred"),
        col("Polyphen2_HVAR_rankscore"),
      split_semicolon("FATHMM_score"),
      split_semicolon("FATHMM_pred"),
      col("FATHMM_converted_rankscore"),
      col("REVEL_rankscore"),
      col("LRT_converted_rankscore"),
      col("LRT_pred"),
      col("CADD_raw"),
      col("CADD_raw_rankscore"),
      col("CADD_phred"),
      col("DANN_score"),
      col("DANN_rankscore"),
      col("phyloP100way_vertebrate"),
      col("phyloP100way_vertebrate_rankscore"),
      col("phyloP30way_mammalian"),
      col("phyloP30way_mammalian_rankscore"),
      col("phyloP17way_primate"),
      col("phyloP17way_primate_rankscore"),
      col("phastCons100way_vertebrate"),
      col("phastCons100way_vertebrate_rankscore"),
      col("phastCons30way_mammalian"),
      col("phastCons30way_mammalian_rankscore"),
      col("phastCons17way_primate"),
      col("phastCons17way_primate_rankscore"),
      col("GERP++_NR"),
      col("GERP++_RS"),
      col("GERP++_RS_rankscore"),

      col("MutPred_rankscore"),
      col("MutPred_score"),

      split_semicolon("MutationAssessor_pred"),
      split_semicolon("MutationAssessor_score"),
      col("MutationAssessor_rankscore"),
      col("MutationTaster_converted_rankscore"),

      split_semicolon("PROVEAN_pred"),
      split_semicolon("PROVEAN_score"),
      col("PROVEAN_converted_rankscore"),
      split_semicolon("VEST4_score"),
      col("VEST4_rankscore"),
      col("MetaSVM_pred"),
      col("MetaSVM_rankscore"),
      col("MetaSVM_score"),
      col("MetaLR_pred"),
      col("MetaLR_rankscore"),
      col("MetaLR_score"),
      col("M-CAP_pred"),
      col("M-CAP_rankscore"),
      col("M-CAP_score"),
      split_semicolon("MPC_score"),
      col("MPC_rankscore"),
      split_semicolon("MVP_score"),
      col("MVP_rankscore"),
      col("PrimateAI_pred"),
      col("PrimateAI_rankscore"),
      col("PrimateAI_score"),
      split_semicolon("DEOGEN2_pred"),
      split_semicolon("DEOGEN2_score"),
      col("DEOGEN2_rankscore"),
      col("BayesDel_addAF_pred"),
      col("BayesDel_addAF_rankscore"),
      col("BayesDel_addAF_score"),
      col("BayesDel_noAF_pred"),
      col("BayesDel_noAF_rankscore"),
      col("BayesDel_noAF_score"),
      col("ClinPred_pred"),
      col("ClinPred_rankscore"),
      col("ClinPred_score"),
      split_semicolon("LIST-S2_pred"),
      split_semicolon("LIST-S2_score"),
      col("LIST-S2_rankscore"),
      col("fathmm-MKL_coding_pred"),
      col("fathmm-MKL_coding_rankscore"),
      col("fathmm-MKL_coding_score"),
      col("fathmm-MKL_coding_group"),
      col("fathmm-XF_coding_pred"),
      col("fathmm-XF_coding_rankscore"),
      col("fathmm-XF_coding_score"),
      col("Eigen-PC-phred_coding"),
      col("Eigen-PC-raw_coding"),
      col("Eigen-PC-raw_coding_rankscore"),
      col("Eigen-phred_coding"),
      col("Eigen-raw_coding"),
      col("Eigen-raw_coding_rankscore"),
      col("GenoCanyon_rankscore"),
      col("GenoCanyon_score"),
      col("integrated_confidence_value"),
      col("integrated_fitCons_rankscore"),
      col("integrated_fitCons_score"),
      col("GM12878_confidence_value"),
      col("GM12878_fitCons_rankscore"),
      col("GM12878_fitCons_score"),
      col("H1-hESC_confidence_value"),
      col("H1-hESC_fitCons_rankscore"),
      col("H1-hESC_fitCons_score"),
      col("HUVEC_confidence_value"),
      col("HUVEC_fitCons_rankscore"),
      col("HUVEC_fitCons_score"),
      col("LINSIGHT"),
      col("LINSIGHT_rankscore"),
      col("bStatistic"),
      col("bStatistic_converted_rankscore"),
      split_semicolon("Interpro_domain"),
      split_semicolon("GTEx_V8_gene"),
      split_semicolon("GTEx_V8_tissue")
    )
      .withColumnRenamed("col", "ensembl_transcript_id")
      .withColumn("position", col("pos") + 1)
      .drop("pos")
      .select(
        col("chromosome"),
        col("start"),
        col("reference"),
        col("alternate"),
        col("aaref"),
        element_at_postion("symbol"),
        element_at_postion("ensembl_gene_id"),
        element_at_postion("ensembl_protein_id"),
        element_at_postion("VEP_canonical"),
        col("ensembl_transcript_id"),
        element_at_postion("cds_strand").cast(IntegerType),
        score("SIFT_score"),
        pred("SIFT_pred"),
        cast("SIFT_converted_rankscore"),

        score("Polyphen2_HDIV_score"),
        pred("Polyphen2_HDIV_pred"),
        cast("Polyphen2_HDIV_rankscore"),

        score("Polyphen2_HVAR_score"),
        pred("Polyphen2_HVAR_pred"),
        cast("Polyphen2_HVAR_rankscore"),

        score("FATHMM_score"),
        pred("FATHMM_pred"),
        cast("FATHMM_converted_rankscore"),

        cast("CADD_raw"),
        cast("CADD_raw_rankscore"),
        cast("CADD_phred"),
        cast("DANN_score"),
        cast("DANN_rankscore"),

        cast("REVEL_rankscore"),
        cast("LRT_converted_rankscore"),
        col("LRT_pred"),
        cast("phyloP100way_vertebrate"),
        cast("phyloP100way_vertebrate_rankscore"),
        cast("phyloP30way_mammalian"),
        cast("phyloP30way_mammalian_rankscore"),
        cast("phyloP17way_primate"),
        cast("phyloP17way_primate_rankscore"),
        cast("phastCons100way_vertebrate"),
        cast("phastCons100way_vertebrate_rankscore"),
        cast("phastcons30way_mammalian"),
        cast("phastCons30way_mammalian_rankscore"),
        cast("phastCons17way_primate"),
        cast("phastCons17way_primate_rankscore"),
        cast("GERP++_NR"),
        cast("GERP++_RS"),
        cast("GERP++_RS_rankscore"),
        cast("MutPred_rankscore"),
        cast("MutPred_score"),
        pred("MutationAssessor_pred"),
        score("MutationAssessor_score"),
        cast("MutationAssessor_rankscore"),
        cast("MutationTaster_converted_rankscore"),
        pred("PROVEAN_pred"),
        score("PROVEAN_score"),
        cast("PROVEAN_converted_rankscore"),
        score("VEST4_score"),
        cast("VEST4_rankscore"),
        col("MetaSVM_pred"),
        cast("MetaSVM_rankscore"),
        cast("MetaSVM_score"),
        col("MetaLR_pred"),
        cast("MetaLR_rankscore"),
        cast("MetaLR_score"),
        col("M-CAP_pred"),
        cast("M-CAP_score"),
        cast("M-CAP_rankscore"),
        score("MPC_score"),
        cast("MPC_rankscore"),
        score("MVP_score"),
        cast("MVP_rankscore"),
        col("PrimateAI_pred"),
        cast("PrimateAI_rankscore"),
        cast("PrimateAI_score"),
        pred("DEOGEN2_pred"),
        score("DEOGEN2_score"),
        cast("DEOGEN2_rankscore"),
        col("BayesDel_addAF_pred"),
        cast("BayesDel_addAF_rankscore"),
        cast("BayesDel_addAF_score"),
        col("BayesDel_noAF_pred"),
        cast("BayesDel_noAF_rankscore"),
        cast("BayesDel_noAF_score"),
        col("ClinPred_pred"),
        cast("ClinPred_rankscore"),
        cast("ClinPred_score"),
        pred("LIST-S2_pred"),
        score("LIST-S2_score"),
        cast("LIST-S2_rankscore"),
        col("fathmm-MKL_coding_pred"),
        cast("fathmm-MKL_coding_rankscore"),
        cast("fathmm-MKL_coding_score"),
        col("fathmm-MKL_coding_group"),
        col("fathmm-XF_coding_pred"),
        cast("fathmm-XF_coding_rankscore"),
        cast("fathmm-XF_coding_score"),
        cast("Eigen-PC-phred_coding"),
        cast("Eigen-PC-raw_coding"),
        cast("Eigen-PC-raw_coding_rankscore"),
        cast("Eigen-phred_coding"),
        cast("Eigen-raw_coding"),
        cast("Eigen-raw_coding_rankscore"),
        cast("GenoCanyon_rankscore"),
        cast("GenoCanyon_score"),
        cast("integrated_confidence_value"),
        cast("integrated_fitCons_rankscore"),
        cast("integrated_fitCons_score"),
        cast("GM12878_confidence_value"),
        cast("GM12878_fitCons_rankscore"),
        cast("GM12878_fitCons_score"),
        cast("H1-hESC_confidence_value"),
        cast("H1-hESC_fitCons_rankscore"),
        cast("H1-hESC_fitCons_score"),
        cast("HUVEC_confidence_value"),
        cast("HUVEC_fitCons_rankscore"),
        cast("HUVEC_fitCons_score"),
        cast("LINSIGHT"),
        cast("LINSIGHT_rankscore"),
        cast("bStatistic"),
        cast("bStatistic_converted_rankscore"),
        element_at_postion("Interpro_domain"),
        col("GTEx_V8_gene"),
        col("GTEx_V8_tissue")
      )
      .drop("position")

  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      .repartition(col("chromosome"))
      .sortWithinPartitions("start")
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("parquet")
      .option("path", destination.path)
      .saveAsTable(s"${destination.database}.${destination.name}")
    data
  }
}

