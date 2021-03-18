package org.kidsfirstdrc.dwh.es.json

import bio.ferlab.datalake.core.etl.DataSource
import bio.ferlab.datalake.core.loader.Formats._
import bio.ferlab.datalake.core.loader.LoadTypes._

object EsCatalog {

  val alias = "kf-strides-variant"

  object Clinical {
    val variants     = DataSource(alias, "/variants/variants_re_*"        , "variant", "variants"    , PARQUET, Upsert)
    val consequences = DataSource(alias, "/consequences/consequences_re_*", "variant", "consequences", PARQUET, Upsert)
  }

  object Public {
    val relativePath = "/public"
    val genes = DataSource(alias, s"$relativePath/Genes", "variant", "genes", PARQUET, Upsert)
  }

  object Es {
    val relativePath = s"/es_index"
    val gene_centric        = DataSource(alias, s"$relativePath/gene_centric"       , "", "gene_centric"       , JSON, OverWrite)
    val genomic_suggestions = DataSource(alias, s"$relativePath/genomic_suggestions", "", "genomic_suggestions", JSON, OverWrite)
  }

}
