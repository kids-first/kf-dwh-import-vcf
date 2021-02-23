package org.kidsfirstdrc.dwh.conf

import org.kidsfirstdrc.dwh.conf.Formats._

object Catalog {

  val kfStridesVariantBucket = "s3a://kf-strides-variant-parquet-prd"

  object Raw extends StoreFolder {
    override val bucket: String  = kfStridesVariantBucket

    val `1000genomes_vcf`         = DataSource("1000genomes_vcf"          , "", bucket, "/raw/1000Genomes/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz", VCF)
    val annovar_dbnsfp            = DataSource("annovar_dbnsfp"           , "", bucket, "/raw/annovar/dbNSFP/hg38_dbnsfp41a.txt"      , CSV)
    val cancerhotspots_csv        = DataSource("cancerhotspots_csv"       , "", bucket, "/raw/cancerhotspots/cancerhotspots.v2.maf.gz", CSV)
    val clinvar_vcf               = DataSource("clinvar_vcf"              , "", bucket, "/raw/clinvar/clinvar.vcf.gz"                 , VCF)
    val cosmic_cancer_gene_census = DataSource("cosmic_cancer_gene_census", "", bucket, "/raw/cosmic/cancer_gene_census.csv"          , CSV)
    val dbsnp_vcf                 = DataSource("dbsnp_vcf"                , "", bucket, "/raw/dbsnp/GCF_000001405.38.gz"              , VCF)
    val dbNSFP_csv                = DataSource("dbNSFP_csv"               , "", bucket, "/raw/dbNSFP/*.gz"                            , CSV)
    val ddd_gene_census           = DataSource("ddd_gene_census"          , "", bucket, "/raw/ddd/DDG2P_17_5_2020.csv"                , CSV)
    val hpo_genes_to_phenotype    = DataSource("hpo_genes_to_phenotype"   , "", bucket, "/raw/hpo/genes_to_phenotype.txt"             , CSV)
    val omim_genemap2             = DataSource("omim_genemap2"            , "", bucket, "/raw/omim/genemap2.txt"                      , CSV)
    val orphanet_gene_association = DataSource("en_product6"              , "", bucket, "/raw/orphanet/en_product6.xml"               , XML)
    val orphanet_disease_history  = DataSource("en_product9_ages"         , "", bucket, "/raw/orphanet/en_product9_ages.xml"          , XML)
    val refseq_homo_sapiens_gene  = DataSource("refseq_homo_sapiens_gene" , "", bucket, "/raw/refseq/Homo_sapiens.gene_info.gz"       , CSV)
    val topmed_bravo_dbsnp        = DataSource("topmed_bravo_dbsnp"       , "", bucket, "/raw/topmed/bravo-dbsnp-all.vcf.gz"          , VCF)
  }

  object Public extends StoreFolder {

    import Raw._

    override val bucket: String  = kfStridesVariantBucket

    val `1000_genomes`    = DataSource("1000_genomes"     , "variant", bucket, "/public/1000_genomes"     , PARQUET, List(`1000genomes_vcf`))
    val cancer_hotspots   = DataSource("cancer_hotspots"  , "variant", bucket, "/public/cancer_hotspots"  , PARQUET, List(cancerhotspots_csv))
    val clinvar           = DataSource("clinvar"          , "variant", bucket, "/public/clinvar"          , PARQUET, List(clinvar_vcf))
    val cosmic_gene_set   = DataSource("cosmic_gene_set"  , "variant", bucket, "/public/cosmic_gene_set"  , PARQUET, List(cosmic_cancer_gene_census))
    val dbnsfp_variant    = DataSource("bdnsfp"           , "variant", bucket, "/public/dbnsfp/variant"   , PARQUET, List(dbNSFP_csv))
    val dbnsfp_annovar    = DataSource("dbnsfp_annovar"   , "variant", bucket, "/public/annovar/dbnsfp"   , PARQUET, List(annovar_dbnsfp))
    val dbnsfp_original   = DataSource("dbnsfp_original"  , "variant", bucket, "/public/dbnsfp/scores"    , PARQUET, List(dbnsfp_variant))
    val dbsnp             = DataSource("dbsnp"            , "variant", bucket, "/public/dbsnp"            , PARQUET, List(dbsnp_vcf))
    val ddd_gene_set      = DataSource("ddd_gene_set"     , "variant", bucket, "/public/ddd_gene_set"     , PARQUET, List(ddd_gene_census))
    val human_genes       = DataSource("human_genes"      , "variant", bucket, "/public/human_genes"      , PARQUET, List(refseq_homo_sapiens_gene))
    val hpo_gene_set      = DataSource("hpo_gene_set"     , "variant", bucket, "/public/hpo_gene_set"     , PARQUET, List(hpo_genes_to_phenotype, human_genes))
    val omim_gene_set     = DataSource("omim_gene_set"    , "variant", bucket, "/public/omim_gene_set"    , PARQUET, List(omim_genemap2))
    val orphanet_gene_set = DataSource("orphanet_gene_set", "variant", bucket, "/public/orphanet_gene_set", PARQUET, List(orphanet_gene_association, orphanet_disease_history))
    val topmed_bravo      = DataSource("topmed_bravo"     , "variant", bucket, "/public/topmed_bravo"     , PARQUET, List(topmed_bravo_dbsnp))
  }

  object Clinical extends StoreFolder {

    import Public._

    override val bucket: String  = kfStridesVariantBucket

    val consequences = DataSource("consequences", "variant", bucket, "/consequences/consequences_re_*", PARQUET, List())
    val occurrences  = DataSource("occurrences" , "variant", bucket, "/occurrences/occurrences_re_*"  , PARQUET, List())
    val variants     = DataSource("variants"    , "variant", bucket, "/variants/variants_re_*"        , PARQUET, List(clinvar, topmed_bravo))
  }

  object ElasticsearchJson extends StoreFolder {

    override val bucket: String = kfStridesVariantBucket

    import Clinical._
    import Public._

    val variantsJson = DataSource("variants_index", "variant", bucket, "/es_index/variants_index_re_*", JSON, List(variants, consequences, omim_gene_set, orphanet_gene_set, ddd_gene_set, cosmic_gene_set))
  }

  def sources: Set[DataSource] = Raw.sources ++ Public.sources ++ Clinical.sources ++ ElasticsearchJson.sources

}
