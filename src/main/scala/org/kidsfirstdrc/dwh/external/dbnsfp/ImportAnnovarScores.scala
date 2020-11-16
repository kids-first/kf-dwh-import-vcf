package org.kidsfirstdrc.dwh.external.dbnsfp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ImportAnnovarScores extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import DBSNFP Scores fron Annovar to DWH").getOrCreate()

  import spark.implicits._

  spark.read
    .option("sep", "\t")
    .option("header", "true")
    .option("nullValue", ".")
    .csv("s3a://kf-strides-variant-parquet-prd/raws/annovar/dbNSFP/hg38_dbnsfp41a.txt")
    .select(
      $"#Chr" as "chromosome",
      $"Start".cast("long") as "start",
      $"End".cast("long") as "end",
      $"Ref" as "reference",
      $"Alt" as "alternate",
      $"DamagePredCount".cast("float") as "DamagePredCount",
      $"SIFT_pred",
      $"SIFT4G_pred",
      $"Polyphen2_HDIV_pred",
      $"Polyphen2_HVAR_pred",
      $"LRT_pred",
      $"MutationTaster_pred",
      $"MutationAssessor_pred",
      $"FATHMM_pred",
      $"PROVEAN_pred",
      $"VEST4_score".cast("float") as "VEST4_score",
      $"MetaSVM_pred",
      $"MetaLR_pred",
      $"M-CAP_pred",
      $"REVEL_score".cast("float") as "REVEL_score",
      $"MutPred_score".cast("float") as "MutPred_score",
      $"MVP_score".cast("float") as "MVP_score",
      $"MPC_score".cast("float") as "MPC_score",
      $"PrimateAI_pred",
      $"DEOGEN2_pred",
      $"BayesDel_addAF_pred",
      $"BayesDel_noAF_pred",
      $"ClinPred_pred",
      $"LIST-S2_pred",
      $"CADD_raw",
      $"CADD_phred",
      $"DANN_score".cast("float") as "DANN_score",
      $"fathmm-MKL_coding_pred",
      $"fathmm-XF_coding_pred",
      $"Eigen-raw_coding",
      $"Eigen-phred_coding",
      $"Eigen-PC-raw_coding",
      $"Eigen-PC-phred_coding",
      $"GenoCanyon_score".cast("float") as "GenoCanyon_score",
      $"integrated_fitCons_score".cast("float") as "integrated_fitCons_score",
      $"GM12878_fitCons_score".cast("float") as "GM12878_fitCons_score",
      $"H1-hESC_fitCons_score".cast("float") as "H1-hESC_fitCons_score",
      $"HUVEC_fitCons_score".cast("float") as "HUVEC_fitCons_score",
      $"LINSIGHT",
      $"GERP++_NR".cast("float") as "GERP++_NR",
      $"GERP++_RS".cast("float") as "GERP++_RS",
      $"phyloP100way_vertebrate".cast("float") as "phyloP100way_vertebrate",
      $"phyloP30way_mammalian".cast("float") as "phyloP30way_mammalian",
      $"phyloP17way_primate".cast("float") as "phyloP17way_primate",
      $"phastCons100way_vertebrate".cast("float") as "phastCons100way_vertebrate",
      $"phastCons30way_mammalian".cast("float") as "phastCons30way_mammalian",
      $"phastCons17way_primate".cast("float") as "phastCons17way_primate",
      $"bStatistic".cast("float") as "bStatistic",
      $"Interpro_domain",
      split($"GTEx_V8_gene", ";") as "GTEx_V8_gene",
      split($"GTEx_V8_tissue", ";") as "GTEx_V8_tissue"
    )
    .repartition($"chromosome")
    .sortWithinPartitions("start")
    .write.mode("overwrite")
    .partitionBy("chromosome")
    .format("parquet")
    .option("path", "s3a://kf-strides-variant-parquet-prd/public/annovar/dbnsfp")
    .saveAsTable("variant.dbnsfp_annovar")


}

