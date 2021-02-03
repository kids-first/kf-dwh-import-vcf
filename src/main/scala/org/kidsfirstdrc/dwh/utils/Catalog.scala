package org.kidsfirstdrc.dwh.utils

import org.kidsfirstdrc.dwh.utils.Formats.{PARQUET, XML}

object Catalog {

  object Raw {
    val bucket = "s3a://kf-strides-variant-parquet-prd"

    object Orphanet {
      val gene_association = DataSource("en_product6"        , "", bucket, "/raw/orphanet/en_product6.xml"      , XML)
      val disease_history  = DataSource("en_product9_ages"   , "", bucket, "/raw/orphanet/en_product9_ages.xml" , XML)
    }
  }

  object Public {
    import Raw.Orphanet._
    val bucket = "s3a://kf-strides-variant-parquet-prd"

    val orphanet_gene_set = DataSource("orphanet_gene_set", "variant", bucket, "/orphanet_gene_set", PARQUET, List(gene_association, disease_history))

  }

}