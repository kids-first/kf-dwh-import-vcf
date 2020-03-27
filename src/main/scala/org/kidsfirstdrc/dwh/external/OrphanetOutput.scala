package org.kidsfirstdrc.dwh.external

case class OrphanetOutput(
                           disorder_id: Int,
                           orpha_number: Int,
                           expert_link: Option[String],
                           name: String,
                           disorder_type: String,
                           disorder_group: String,
                           gene_symbol: String,
                           ensembl_id: Option[String],
                           association_type: Option[String],
                           association_type_id: Option[Int],
                           status: Option[String])