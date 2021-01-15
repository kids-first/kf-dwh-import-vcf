package org.kidsfirstdrc.dwh.utils

import org.kidsfirstdrc.dwh.utils.Formats.{CSV, PARQUET}

object Catalog {

  object Raw {
    val root = "s3a://kf-strides-variant-parquet-prd/raw"

    val hg38_dbnsfp41a = DataSource("hg38_dbnsfp41a", "", s"$root/annovar/dbNSFP/hg38_dbnsfp41a.txt", CSV)
    val dbNSFP         = DataSource("dbNSFP"        , "", s"$root/annovar/dbNSFP/*.gz"              , CSV)
    val genemap2       = DataSource("genemap2"      , "", s"$root/omim/genemap2.txt"                , CSV)
  }

  object Public {
    import Raw._
    val root = "s3a://kf-strides-variant-parquet-prd/public"

    val dbnsfp_annovar  = DataSource("dbnsfp_annovar" , "variant", s"$root/annovar/dbnsfp"        , PARQUET, List(hg38_dbnsfp41a))
    val dbnsfp          = DataSource("dbnsfp"         , "variant", s"$root/annovar/dbnsfp/variant", PARQUET, List(dbNSFP))
    val dbnsfp_original = DataSource("dbnsfp_original", "variant", s"$root/annovar/dbnsfp/scores" , PARQUET, List(dbnsfp))
    val omim_gene_set   = DataSource("omim_gene_set"  , "variant", s"$root/omim_gene_set"         , PARQUET, List(genemap2))
  }

}
