package org.kidsfirstdrc.dwh.conf

import bio.ferlab.datalake.spark3.config.SourceConf
import bio.ferlab.datalake.spark3.loader.Format._
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite

object Catalog {

  val kfStridesVariantBucket = "kf-strides-variant"

  val kfStudyBucket = "kf-study"

  object HarmonizedData extends StoreFolder {
    override val alias: String = kfStudyBucket
    val family_variants_vcf = SourceConf(alias, "", "", "", VCF, OverWrite)
  }

  object Raw extends StoreFolder {
    override val alias: String  = kfStridesVariantBucket

    val `1000genomes_vcf`         = SourceConf(alias, "/raw/1000Genomes/ALL.*.sites.vcf.gz"        , "", "1000genomes_vcf"          ,    VCF , OverWrite)
    val annovar_dbnsfp            = SourceConf(alias, "/raw/annovar/dbNSFP/hg38_dbnsfp41a.txt"     , "", "annovar_dbnsfp"           ,    CSV , OverWrite)
    val cancerhotspots_csv        = SourceConf(alias, "/raw/cancerhotspots/cancerhotspots.*.gz"    , "", "cancerhotspots_csv"       ,    CSV , OverWrite)
    val clinvar_vcf               = SourceConf(alias, "/raw/clinvar/clinvar.vcf.gz"                , "", "clinvar_vcf"              ,    VCF , OverWrite)
    val cosmic_cancer_gene_census = SourceConf(alias, "/raw/cosmic/cancer_gene_census.csv"         , "", "cosmic_cancer_gene_census",    CSV , OverWrite)
    val dbsnp_vcf                 = SourceConf(alias, "/raw/dbsnp/GCF_000001405.38.gz"             , "", "dbsnp_vcf"                ,    VCF , OverWrite)
    val dbNSFP_csv                = SourceConf(alias, "/raw/dbNSFP/*.gz"                           , "", "dbNSFP_csv"               ,    CSV , OverWrite)
    val ddd_gene_census           = SourceConf(alias, "/raw/ddd/DDG2P_17_5_2020.csv"               , "", "ddd_gene_census"          ,    CSV , OverWrite)
    val ensembl_canonical         = SourceConf(alias, "/raw/ensembl/*.GRCh38.104.canonical.tsv.gz" , "", "ensembl_canonical"        ,    CSV , OverWrite)
    val ensembl_entrez            = SourceConf(alias, "/raw/ensembl/*.GRCh38.104.entrez.tsv.gz"    , "", "ensembl_entrez"           ,    CSV , OverWrite)
    val ensembl_ena               = SourceConf(alias, "/raw/ensembl/*.GRCh38.104.ena.tsv.gz"       , "", "ensembl_ena"              ,    CSV , OverWrite)
    val ensembl_refseq            = SourceConf(alias, "/raw/ensembl/*.GRCh38.104.refseq.tsv.gz"    , "", "ensembl_refseq"           ,    CSV , OverWrite)
    val ensembl_uniprot           = SourceConf(alias, "/raw/ensembl/*.GRCh38.104.uniprot.tsv.gz"   , "", "ensembl_uniprot"          ,    CSV , OverWrite)
    val hpo_genes_to_phenotype    = SourceConf(alias, "/raw/hpo/genes_to_phenotype.txt"            , "", "hpo_genes_to_phenotype"   ,    CSV , OverWrite)
    val omim_genemap2             = SourceConf(alias, "/raw/omim/genemap2.txt"                     , "", "omim_genemap2"            ,    CSV , OverWrite)
    val orphanet_gene_association = SourceConf(alias, "/raw/orphanet/en_product6.xml"              , "", "en_product6"              ,    XML , OverWrite)
    val orphanet_disease_history  = SourceConf(alias, "/raw/orphanet/en_product9_ages.xml"         , "", "en_product9_ages"         ,    XML , OverWrite)
    val refseq_homo_sapiens_gene  = SourceConf(alias, "/raw/refseq/Homo_sapiens.gene_info.gz"      , "", "refseq_homo_sapiens_gene" ,    CSV , OverWrite)
    val topmed_bravo_dbsnp        = SourceConf(alias, "/raw/topmed/bravo-dbsnp-all.vcf.gz"         , "", "topmed_bravo_dbsnp"       ,    VCF , OverWrite)
    val all_participants          = SourceConf(alias, "/raw/participants/all_participants_*.json"  , "", "all_participants"         ,    JSON, OverWrite)

  }

  object Public extends StoreFolder {

    override val alias: String  = kfStridesVariantBucket

    val `1000_genomes`     = SourceConf(alias, "/public/1000_genomes"      , "variant", "1000_genomes"      , PARQUET, OverWrite)
    val cancer_hotspots    = SourceConf(alias, "/public/cancer_hotspots"   , "variant", "cancer_hotspots"   , PARQUET, OverWrite)
    val clinvar            = SourceConf(alias, "/public/clinvar"           , "variant", "clinvar"           , PARQUET, OverWrite)
    val cosmic_gene_set    = SourceConf(alias, "/public/cosmic_gene_set"   , "variant", "cosmic_gene_set"   , PARQUET, OverWrite)
    val dbnsfp_variant     = SourceConf(alias, "/public/dbnsfp/variant"    , "variant", "dbnsfp"            , PARQUET, OverWrite)
    val dbnsfp_annovar     = SourceConf(alias, "/public/annovar/dbnsfp"    , "variant", "dbnsfp_annovar"    , PARQUET, OverWrite)
    val dbnsfp_original    = SourceConf(alias, "/public/dbnsfp/scores"     , "variant", "dbnsfp_original"   , PARQUET, OverWrite)
    val dbsnp              = SourceConf(alias, "/public/dbsnp"             , "variant", "dbsnp"             , PARQUET, OverWrite)
    val ddd_gene_set       = SourceConf(alias, "/public/ddd_gene_set"      , "variant", "ddd_gene_set"      , PARQUET, OverWrite)
    val ensembl_mapping    = SourceConf(alias, "/public/ensembl_mapping"   , "variant", "ensembl_mapping"   , PARQUET, OverWrite)
    val genes              = SourceConf(alias, "/public/genes"             , "variant", "genes"             , PARQUET, OverWrite)
    val gnomad_genomes_2_1 = SourceConf(alias, "/public/gnomad_genomes_2_1_1_liftover_grch38", "variant", "gnomad_genomes_2_1_1_liftover_grch38", PARQUET, OverWrite)
    val gnomad_exomes_2_1  = SourceConf(alias, "/public/gnomad_exomes_2_1_1_liftover_grch38" , "variant", "gnomad_exomes_2_1_1_liftover_grch38" , PARQUET, OverWrite)
    val gnomad_genomes_3_0 = SourceConf(alias, "/public/gnomad_genomes_3_0", "variant", "gnomad_genomes_3_0", PARQUET, OverWrite)
    val human_genes        = SourceConf(alias, "/public/human_genes"       , "variant", "human_genes"       , PARQUET, OverWrite)
    val hpo_gene_set       = SourceConf(alias, "/public/hpo_gene_set"      , "variant", "hpo_gene_set"      , PARQUET, OverWrite)
    val omim_gene_set      = SourceConf(alias, "/public/omim_gene_set"     , "variant", "omim_gene_set"     , PARQUET, OverWrite)
    val orphanet_gene_set  = SourceConf(alias, "/public/orphanet_gene_set" , "variant", "orphanet_gene_set" , PARQUET, OverWrite)
    val topmed_bravo       = SourceConf(alias, "/public/topmed_bravo"      , "variant", "topmed_bravo"      , PARQUET, OverWrite)
  }

  object DataService extends StoreFolder {

    override val alias: String  = kfStridesVariantBucket

    val studies                = SourceConf(alias, "/dataservice/studies/studies_re_*"                          , "variant", "studies"               , PARQUET, OverWrite)
    val biospecimens           = SourceConf(alias, "/dataservice/biospecimens/biospecimens_re_*"                , "variant", "biospecimens"          , PARQUET, OverWrite)
    val family_relationships   = SourceConf(alias, "/dataservice/family_relationships/family_relationships_re_*", "variant", "family_relationships"  , PARQUET, OverWrite)
    val participants           = SourceConf(alias, "/dataservice/participants/participants_re_*"                , "variant", "participants"          , PARQUET, OverWrite)
    val genomic_files          = SourceConf(alias, "/dataservice/genomic_files/genomic_files_re_*"              , "variant", "genomic_files"         , PARQUET, OverWrite)
    val genomic_files_override = SourceConf(alias, "/genomic_files_override"                                    , "variant", "genomic_files_override", CSV    , OverWrite)

  }

  object Clinical extends StoreFolder {

    override val alias: String  = kfStridesVariantBucket

    val consequences        = SourceConf(alias, "/consequences/consequences_re_*"          , "variant", "consequences"      , PARQUET, OverWrite)
    val occurrences         = SourceConf(alias, "/occurrences/occurrences_sd_*_re_*"       , "variant", "occurrences"       , PARQUET, OverWrite)
    val occurrences_family  = SourceConf(alias, "/occurrences/occurrences_family_sd_*_re_*", "variant", "occurrences_family", PARQUET, OverWrite)
    val variants            = SourceConf(alias, "/variants/variants_re_*"                  , "variant", "variants"          , PARQUET, OverWrite)
  }

  object Es extends StoreFolder {

    override val alias: String = kfStridesVariantBucket
    val relativePath = s"/es_index"
    val variant_centric     = SourceConf(alias, s"$relativePath/variant_centric"     , "portal", "variant_centric"    , PARQUET, OverWrite)
    val gene_centric        = SourceConf(alias, s"$relativePath/gene_centric"        , "portal", "gene_centric"       , PARQUET, OverWrite)
    val genomic_suggestions = SourceConf(alias, s"$relativePath/genomic_suggestions" , "portal", "genomic_suggestions", PARQUET, OverWrite)
  }

  def sources: Set[SourceConf] = Raw.sources ++ Public.sources ++ Clinical.sources ++ Es.sources

}
