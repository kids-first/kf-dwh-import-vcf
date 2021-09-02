package org.kidsfirstdrc.dwh.external.dbnsfp

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.jobs.StandardETL

import java.time.LocalDateTime

class ImportAnnovarScores()(implicit conf: Configuration)
    extends StandardETL(Public.dbnsfp_annovar)(conf) {

  val source: DatasetConf = Raw.annovar_dbnsfp

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val annovar_dbnsfp = spark.read
      .option("sep", "\t")
      .option("header", "true")
      .option("nullValue", ".")
      .csv(source.location)

    Map(source.id -> annovar_dbnsfp)
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(source.id)
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
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(col("chromosome"))
      .sortWithinPartitions("start"))
  }
}
