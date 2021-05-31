package org.kidsfirstdrc.dwh.external.orphanet

case class OrphanetGeneAssociation(
    disorder_id: Long,
    orpha_code: Long,
    expert_link: String,
    name: String,
    disorder_type_id: Long,
    disorder_type_name: String,
    disorder_group_id: Long,
    disorder_group_name: String,
    gene_source_of_validation: String,
    gene_id: Long,
    gene_symbol: String,
    gene_name: String,
    gene_synonym_list: List[String],
    ensembl_gene_id: Option[String],
    genatlas_gene_id: Option[String],
    HGNC_gene_id: Option[String],
    omim_gene_id: Option[String],
    reactome_gene_id: Option[String],
    swiss_prot_gene_id: Option[String],
    association_type: Option[String],
    association_type_id: Option[Long],
    association_status: Option[String],
    gene_locus_id: Long,
    gene_locus: String,
    gene_locus_key: Long
)
