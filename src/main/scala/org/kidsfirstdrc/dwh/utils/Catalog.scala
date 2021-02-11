package org.kidsfirstdrc.dwh.utils

import org.kidsfirstdrc.dwh.utils.Formats.{CSV, PARQUET, VCF, XML}

object Catalog {

  object Raw  extends StoreFolder {
    val bucket = "s3a://kf-strides-variant-parquet-prd"

    val annovar_dbnsfp            = DataSource("annovar_dbnsfp"           , "", bucket, "/raw/annovar/dbNSFP/hg38_dbnsfp41a.txt", CSV)
    val clinvar_vcf               = DataSource("clinvar_vcf"              , "", bucket, "/raw/clinvar/clinvar.vcf.gz"           , VCF)
    val cosmic_cancer_gene_census = DataSource("cosmic_cancer_gene_census", "", bucket, "/raw/cosmic/cancer_gene_census.csv"    , CSV)
    val dbNSFP_csv                = DataSource("dbNSFP_csv"               , "", bucket, "/raw/dbNSFP/*.gz"                      , CSV)
    val ddd_gene_census           = DataSource("ddd_gene_census"          , "", bucket, "/raw/ddd/DDG2P_17_5_2020.csv"          , CSV)
    val hpo_genes_to_phenotype    = DataSource("hpo_genes_to_phenotype"   , "", bucket, "/raw/hpo/genes_to_phenotype.txt"       , CSV)
    val omim_genemap2             = DataSource("omim_genemap2"            , "", bucket, "/raw/omim/genemap2.txt"                , CSV)
    val orphanet_gene_association = DataSource("en_product6"              , "", bucket, "/raw/orphanet/en_product6.xml"         , XML)
    val orphanet_disease_history  = DataSource("en_product9_ages"         , "", bucket, "/raw/orphanet/en_product9_ages.xml"    , XML)
    val refseq_homo_sapiens_gene  = DataSource("refseq_homo_sapiens_gene" , "", bucket, "/raw/refseq/Homo_sapiens.gene_info.gz" , CSV)

  }

  object Public extends StoreFolder {
    import Raw._
    val bucket = "s3a://kf-strides-variant-parquet-prd"

    val clinvar           = DataSource("clinvar"          , "variant", bucket, "/public/clinvar"          , PARQUET, List(clinvar_vcf))
    val dbnsfp_annovar    = DataSource("dbnsfp_annovar"   , "variant", bucket, "/public/annovar/dbnsfp"   , PARQUET, List(annovar_dbnsfp))
    val omim_gene_set     = DataSource("omim_gene_set"    , "variant", bucket, "/public/omim_gene_set"    , PARQUET, List(omim_genemap2))
    val orphanet_gene_set = DataSource("orphanet_gene_set", "variant", bucket, "/public/orphanet_gene_set", PARQUET, List(orphanet_gene_association, orphanet_disease_history))

  }

  def sources: Set[DataSource] = Raw.sources ++ Public.sources

}