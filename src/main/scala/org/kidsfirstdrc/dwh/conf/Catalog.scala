package org.kidsfirstdrc.dwh.conf

import bio.ferlab.datalake.core.loader.Formats._

object Catalog {

  val kfStridesVariantBucket = "s3a://kf-strides-variant-parquet-prd"

  object Raw extends StoreFolder {
    override val alias: String  = kfStridesVariantBucket

    //val `1000genomes_vcf`         = Ds("1000genomes_vcf"          , "", alias, "/raw/1000Genomes/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz", VCF)
    //val annovar_dbnsfp            = Ds("annovar_dbnsfp"           , "", alias, "/raw/annovar/dbNSFP/hg38_dbnsfp41a.txt"      , CSV)
    val cancerhotspots_csv        = Ds("cancerhotspots_csv"       , "", alias, "/raw/cancerhotspots/cancerhotspots.v2.maf.gz", CSV)
    val clinvar_vcf               = Ds("clinvar_vcf"              , "", alias, "/raw/clinvar/clinvar.vcf.gz"                 , VCF)
    val cosmic_cancer_gene_census = Ds("cosmic_cancer_gene_census", "", alias, "/raw/cosmic/cancer_gene_census.csv"          , CSV)
    val dbsnp_vcf                 = Ds("dbsnp_vcf"                , "", alias, "/raw/dbsnp/GCF_000001405.38.gz"              , VCF)
    //val dbNSFP_csv                = Ds("dbNSFP_csv"               , "", alias, "/raw/dbNSFP/*.gz"                            , CSV)
    val ddd_gene_census           = Ds("ddd_gene_census"          , "", alias, "/raw/ddd/DDG2P_17_5_2020.csv"                , CSV)
    val hpo_genes_to_phenotype    = Ds("hpo_genes_to_phenotype"   , "", alias, "/raw/hpo/genes_to_phenotype.txt"             , CSV)
    val omim_genemap2             = Ds("omim_genemap2"            , "", alias, "/raw/omim/genemap2.txt"                      , CSV)
    val orphanet_gene_association = Ds("en_product6"              , "", alias, "/raw/orphanet/en_product6.xml"               , XML)
    val orphanet_disease_history  = Ds("en_product9_ages"         , "", alias, "/raw/orphanet/en_product9_ages.xml"          , XML)
    val refseq_homo_sapiens_gene  = Ds("refseq_homo_sapiens_gene" , "", alias, "/raw/refseq/Homo_sapiens.gene_info.gz"       , CSV)
    val topmed_bravo_dbsnp        = Ds("topmed_bravo_dbsnp"       , "", alias, "/raw/topmed/bravo-dbsnp-all.vcf.gz"          , VCF)
  }

  object Public extends StoreFolder {

    override val alias: String  = kfStridesVariantBucket

    val `1000_genomes`    = Ds("1000_genomes"     , "variant", alias, "/public/1000_genomes"     , PARQUET)
    val cancer_hotspots   = Ds("cancer_hotspots"  , "variant", alias, "/public/cancer_hotspots"  , PARQUET)
    val clinvar           = Ds("clinvar"          , "variant", alias, "/public/clinvar"          , PARQUET)
    val cosmic_gene_set   = Ds("cosmic_gene_set"  , "variant", alias, "/public/cosmic_gene_set"  , PARQUET)
    val dbnsfp_variant    = Ds("bdnsfp"           , "variant", alias, "/public/dbnsfp/variant"   , PARQUET)
    val dbnsfp_annovar    = Ds("dbnsfp_annovar"   , "variant", alias, "/public/annovar/dbnsfp"   , PARQUET)
    val dbnsfp_original   = Ds("dbnsfp_original"  , "variant", alias, "/public/dbnsfp/scores"    , PARQUET)
    val dbsnp             = Ds("dbsnp"            , "variant", alias, "/public/dbsnp"            , PARQUET)
    val ddd_gene_set      = Ds("ddd_gene_set"     , "variant", alias, "/public/ddd_gene_set"     , PARQUET)
    val genes             = Ds("genes"            , "variant", alias, "/public/genes"            , PARQUET)
    val human_genes       = Ds("human_genes"      , "variant", alias, "/public/human_genes"      , PARQUET)
    val hpo_gene_set      = Ds("hpo_gene_set"     , "variant", alias, "/public/hpo_gene_set"     , PARQUET)
    val omim_gene_set     = Ds("omim_gene_set"    , "variant", alias, "/public/omim_gene_set"    , PARQUET)
    val orphanet_gene_set = Ds("orphanet_gene_set", "variant", alias, "/public/orphanet_gene_set", PARQUET)
    val topmed_bravo      = Ds("topmed_bravo"     , "variant", alias, "/public/topmed_bravo"     , PARQUET)
  }

  object DataService extends StoreFolder {

    override val alias: String  = kfStridesVariantBucket

    val studies = Ds("studies", "variant", alias, "/dataservice/studies/studies_re_*"     , PARQUET)
  }

  object Clinical extends StoreFolder {

    override val alias: String  = kfStridesVariantBucket

    val consequences = Ds("consequences", "variant", alias, "/consequences/consequences_re_*"     , PARQUET)
    val occurrences  = Ds("occurrences" , "variant", alias, "/occurrences/occurrences_sd_*_re_*"  , PARQUET)
    val variants     = Ds("variants"    , "variant", alias, "/variants/variants_re_*"             , PARQUET)
  }

  object ElasticsearchJson extends StoreFolder {

    override val alias: String = kfStridesVariantBucket

    val variantsJson = Ds("variants_index", "variant", alias, "/es_index/variants_index_re_*", JSON)
  }

  def sources: Set[Ds] = Raw.ds ++ Public.ds ++ Clinical.ds ++ ElasticsearchJson.ds

}
