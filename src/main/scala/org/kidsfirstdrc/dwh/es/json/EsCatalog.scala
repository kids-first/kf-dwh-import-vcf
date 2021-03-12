package org.kidsfirstdrc.dwh.es.json

import bio.ferlab.datalake.core.etl.DataSource
import bio.ferlab.datalake.core.etl.Formats.{JSON, PARQUET}

object EsCatalog {

  val alias = "kf-strides-variant"

  object Public {
    val relativePath = "/public"
    val genes = DataSource(alias, s"$relativePath/Genes", "variant", "genes", PARQUET)
  }

  object Es {
    val relativePath = s"/es_index"
    val gene_centric = DataSource(alias, s"$relativePath/gene_centric", "", "gene_centric", JSON)
  }

}
