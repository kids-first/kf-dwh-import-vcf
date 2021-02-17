package org.kidsfirstdrc.dwh.conf

import org.kidsfirstdrc.dwh.conf.Formats._

object Catalog {

  val kfStridesVariantBucket = "s3a://kf-strides-variant-parquet-prd"

  object Raw extends StoreFolder {
    override val bucket: String  = kfStridesVariantBucket

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
    val topmed_bravo_dbsnp        = DataSource("topmed_bravo_dbsnp"       , "", bucket, "/raw/topmed/bravo-dbsnp-all.vcf.gz"    , VCF)
  }

  object Public extends StoreFolder {

    import Raw._

    override val bucket: String  = kfStridesVariantBucket

    val clinvar           = DataSource("clinvar"          , "variant", bucket, "/public/clinvar"          , PARQUET, List(clinvar_vcf))
    val cosmic_gene_set   = DataSource("cosmic_gene_set"  , "variant", bucket, "/public/cosmic_gene_set"  , PARQUET, List(cosmic_cancer_gene_census))
    val dbnsfp_variant    = DataSource("bdnsfp"           , "variant", bucket, "/public/dbnsfp/variant"   , PARQUET, List())
    val dbnsfp_annovar    = DataSource("dbnsfp_annovar"   , "variant", bucket, "/public/annovar/dbnsfp"   , PARQUET, List(annovar_dbnsfp))
    val dbnsfp_original   = DataSource("dbnsfp_original"  , "variant", bucket, "/public/dbnsfp/scores"    , PARQUET, List(dbnsfp_variant))
    val ddd_gene_set      = DataSource("ddd_gene_set"     , "variant", bucket, "/public/ddd_gene_set"     , PARQUET, List(ddd_gene_census))
    val omim_gene_set     = DataSource("omim_gene_set"    , "variant", bucket, "/public/omim_gene_set"    , PARQUET, List(omim_genemap2))
    val orphanet_gene_set = DataSource("orphanet_gene_set", "variant", bucket, "/public/orphanet_gene_set", PARQUET, List(orphanet_gene_association, orphanet_disease_history))
    val topmed_bravo      = DataSource("topmed_bravo"     , "variant", bucket, "/public/topmed_bravo"     , PARQUET, List())
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

    val variantsJson = DataSource("variants_index", "", bucket, "/es_index/variants_index_re_*", JSON, List(variants, consequences, omim_gene_set, orphanet_gene_set, ddd_gene_set, clinvar, cosmic_gene_set))
  }

  def sources: Set[DataSource] = Raw.sources ++ Public.sources

}
