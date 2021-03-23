package org.kidsfirstdrc.dwh.conf

import bio.ferlab.datalake.core.etl.DataSource
import bio.ferlab.datalake.core.loader.Formats._
import bio.ferlab.datalake.core.loader.LoadTypes.OverWrite

object Catalog {

  val kfStridesVariantBucket = "kf-strides-variant"
  val kfStudyBucket = "kf-study"

  object HarmonizedData extends StoreFolder {
    override val alias: String = kfStudyBucket
    val family_variants_vcf = DataSource(alias, "", "", "", VCF, OverWrite)
  }

  object Raw extends StoreFolder {
    override val alias: String  = kfStridesVariantBucket

    val `1000genomes_vcf`         = DataSource(alias, "/raw/1000Genomes/ALL.*.sites.vcf.gz"    , "", "1000genomes_vcf"          ,    VCF, OverWrite)
    val annovar_dbnsfp            = DataSource(alias, "/raw/annovar/dbNSFP/hg38_dbnsfp41a.txt" , "", "annovar_dbnsfp"           ,    CSV, OverWrite)
    val cancerhotspots_csv        = DataSource(alias, "/raw/cancerhotspots/cancerhotspots.*.gz", "", "cancerhotspots_csv"       ,    CSV, OverWrite)
    val clinvar_vcf               = DataSource(alias, "/raw/clinvar/clinvar.vcf.gz"            , "", "clinvar_vcf"              ,    VCF, OverWrite)
    val cosmic_cancer_gene_census = DataSource(alias, "/raw/cosmic/cancer_gene_census.csv"     , "", "cosmic_cancer_gene_census",    CSV, OverWrite)
    val dbsnp_vcf                 = DataSource(alias, "/raw/dbsnp/GCF_000001405.38.gz"         , "", "dbsnp_vcf"                ,    VCF, OverWrite)
    val dbNSFP_csv                = DataSource(alias, "/raw/dbNSFP/*.gz"                       , "", "dbNSFP_csv"               ,    CSV, OverWrite)
    val ddd_gene_census           = DataSource(alias, "/raw/ddd/DDG2P_17_5_2020.csv"           , "", "ddd_gene_census"          ,    CSV, OverWrite)
    val hpo_genes_to_phenotype    = DataSource(alias, "/raw/hpo/genes_to_phenotype.txt"        , "", "hpo_genes_to_phenotype"   ,    CSV, OverWrite)
    val omim_genemap2             = DataSource(alias, "/raw/omim/genemap2.txt"                 , "", "omim_genemap2"            ,    CSV, OverWrite)
    val orphanet_gene_association = DataSource(alias, "/raw/orphanet/en_product6.xml"          , "", "en_product6"              ,    XML, OverWrite)
    val orphanet_disease_history  = DataSource(alias, "/raw/orphanet/en_product9_ages.xml"     , "", "en_product9_ages"         ,    XML, OverWrite)
    val refseq_homo_sapiens_gene  = DataSource(alias, "/raw/refseq/Homo_sapiens.gene_info.gz"  , "", "refseq_homo_sapiens_gene" ,    CSV, OverWrite)
    val topmed_bravo_dbsnp        = DataSource(alias, "/raw/topmed/bravo-dbsnp-all.vcf.gz"     , "", "topmed_bravo_dbsnp"       ,    VCF, OverWrite)
  }

  object Public extends StoreFolder {

    override val alias: String  = kfStridesVariantBucket

    val `1000_genomes`    = DataSource(alias, "/public/1000_genomes"     , "variant", "1000_genomes"     , PARQUET, OverWrite)
    val cancer_hotspots   = DataSource(alias, "/public/cancer_hotspots"  , "variant", "cancer_hotspots"  , PARQUET, OverWrite)
    val clinvar           = DataSource(alias, "/public/clinvar"          , "variant", "clinvar"          , PARQUET, OverWrite)
    val cosmic_gene_set   = DataSource(alias, "/public/cosmic_gene_set"  , "variant", "cosmic_gene_set"  , PARQUET, OverWrite)
    val dbnsfp_variant    = DataSource(alias, "/public/dbnsfp/variant"   , "variant", "bdnsfp"           , PARQUET, OverWrite)
    val dbnsfp_annovar    = DataSource(alias, "/public/annovar/dbnsfp"   , "variant", "dbnsfp_annovar"   , PARQUET, OverWrite)
    val dbnsfp_original   = DataSource(alias, "/public/dbnsfp/scores"    , "variant", "dbnsfp_original"  , PARQUET, OverWrite)
    val dbsnp             = DataSource(alias, "/public/dbsnp"            , "variant", "dbsnp"            , PARQUET, OverWrite)
    val ddd_gene_set      = DataSource(alias, "/public/ddd_gene_set"     , "variant", "ddd_gene_set"     , PARQUET, OverWrite)
    val genes             = DataSource(alias, "/public/genes"            , "variant", "genes"            , PARQUET, OverWrite)
    val human_genes       = DataSource(alias, "/public/human_genes"      , "variant", "human_genes"      , PARQUET, OverWrite)
    val hpo_gene_set      = DataSource(alias, "/public/hpo_gene_set"     , "variant", "hpo_gene_set"     , PARQUET, OverWrite)
    val omim_gene_set     = DataSource(alias, "/public/omim_gene_set"    , "variant", "omim_gene_set"    , PARQUET, OverWrite)
    val orphanet_gene_set = DataSource(alias, "/public/orphanet_gene_set", "variant", "orphanet_gene_set", PARQUET, OverWrite)
    val topmed_bravo      = DataSource(alias, "/public/topmed_bravo"     , "variant", "topmed_bravo"     , PARQUET, OverWrite)
  }

  object DataService extends StoreFolder {

    override val alias: String  = kfStridesVariantBucket

    val studies                = DataSource(alias, "/dataservice/studies/studies_re_*"                          , "variant", "studies"               , PARQUET, OverWrite)
    val biospecimens           = DataSource(alias, "/dataservice/biospecimens/biospecimens_re_*"                , "variant", "biospecimens"          , PARQUET, OverWrite)
    val family_relationships   = DataSource(alias, "/dataservice/family_relationships/family_relationships_re_*", "variant", "family_relationships"  , PARQUET, OverWrite)
    val participants           = DataSource(alias, "/dataservice/participants/participants_re_*"                , "variant", "participants"          , PARQUET, OverWrite)
    val genomic_files          = DataSource(alias, "/dataservice/genomic_files/genomic_files_re_*"              , "variant", "genomic_files"         , PARQUET, OverWrite)
    val genomic_files_override = DataSource(alias, "/genomic_files_override"                                    , "variant", "genomic_files_override", CSV    , OverWrite)

  }

  object Clinical extends StoreFolder {

    override val alias: String  = kfStridesVariantBucket

    val consequences = DataSource(alias, "/consequences/consequences_re_*"   , "variant", "consequences", PARQUET, OverWrite)
    val occurrences  = DataSource(alias, "/occurrences/occurrences_sd_*_re_*", "variant", "occurrences" , PARQUET, OverWrite)
    val variants     = DataSource(alias, "/variants/variants_re_*"           , "variant", "variants"    , PARQUET, OverWrite)
  }

  object Es extends StoreFolder {

    override val alias: String = kfStridesVariantBucket
    val relativePath = s"/es_index"
    val variant_centric     = DataSource(alias, s"$relativePath/variant_centric_re_*", "", "variant_centric"    , JSON, OverWrite)
    val gene_centric        = DataSource(alias, s"$relativePath/gene_centric"        , "", "gene_centric"       , JSON, OverWrite)
    val genomic_suggestions = DataSource(alias, s"$relativePath/genomic_suggestions" , "", "genomic_suggestions", JSON, OverWrite)
  }

  def sources: Set[DataSource] = Raw.sources ++ Public.sources ++ Clinical.sources ++ Es.sources

}
