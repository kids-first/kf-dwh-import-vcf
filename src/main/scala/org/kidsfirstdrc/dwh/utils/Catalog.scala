package org.kidsfirstdrc.dwh.utils

import org.kidsfirstdrc.dwh.utils.Formats.{CSV, PARQUET, VCF, XML}

object Catalog {

  object Raw  extends StoreFolder {
    val bucket = "s3a://kf-strides-variant-parquet-prd"

    val clinvar_vcf               = DataSource("clinvar_vcf"        , "", bucket, "/raw/clinvar/clinvar.vcf.gz"        , VCF)

    val omim_genemap2             = DataSource("omim_genemap2"      , "", bucket, "/raw/omim/genemap2.txt"             , CSV)

    val orphanet_gene_association = DataSource("en_product6"        , "", bucket, "/raw/orphanet/en_product6.xml"      , XML)
    val orphanet_disease_history  = DataSource("en_product9_ages"   , "", bucket, "/raw/orphanet/en_product9_ages.xml" , XML)
  }

  object Public extends StoreFolder {
    import Raw._
    val bucket = "s3a://kf-strides-variant-parquet-prd"

    val omim_gene_set     = DataSource("omim_gene_set"    , "variant", bucket, "/public/omim_gene_set"    , PARQUET, List(omim_genemap2))
    val orphanet_gene_set = DataSource("orphanet_gene_set", "variant", bucket, "/public/orphanet_gene_set", PARQUET, List(orphanet_gene_association, orphanet_disease_history))
    val clinvar           = DataSource("clinvar"          , "variant", bucket, "/public/clinvar"          , PARQUET, List(clinvar_vcf))

  }

  def sources: Set[DataSource] = Raw.sources ++ Public.sources

}