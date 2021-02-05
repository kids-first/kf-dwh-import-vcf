package org.kidsfirstdrc.dwh.utils

import org.kidsfirstdrc.dwh.utils.Formats.{PARQUET, VCF, XML}

object Catalog {

  object Raw {
    val bucket = "s3a://kf-strides-variant-parquet-prd"

    val clinvar_vcf               = DataSource("clinvar"            , "", bucket, "/raw/clinvar/clinvar.vcf.gz"        , VCF)

    val orphanet_gene_association = DataSource("en_product6"        , "", bucket, "/raw/orphanet/en_product6.xml"      , XML)
    val orphanet_disease_history  = DataSource("en_product9_ages"   , "", bucket, "/raw/orphanet/en_product9_ages.xml" , XML)
  }

  object Public {
    import Raw._
    val bucket = "s3a://kf-strides-variant-parquet-prd"

    val orphanet_gene_set = DataSource("orphanet_gene_set", "variant", bucket, "/public/orphanet_gene_set", PARQUET, List(orphanet_gene_association, orphanet_disease_history))
    val clinvar           = DataSource("clinvar"          , "variant", bucket, "/public/clinvar"          , PARQUET, List(clinvar_vcf))

  }

}