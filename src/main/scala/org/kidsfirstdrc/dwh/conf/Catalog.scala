package org.kidsfirstdrc.dwh.conf

import bio.ferlab.datalake.spark3.config.{DatasetConf, TableConf}
import bio.ferlab.datalake.spark3.loader.Format._
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite

object Catalog {

  val kfStridesVariantBucket = "kf-strides-variant"

  val kfStudyBucket = "kf-study"

  object HarmonizedData extends StoreFolder {
    override val alias: String = kfStudyBucket
    val family_variants_vcf    = DatasetConf("family_variants_vcf", alias, "", VCF, OverWrite)
  }

  object Raw extends StoreFolder {
    override val alias: String = kfStridesVariantBucket

    val `1000genomes_vcf` =
      DatasetConf("1000genomes_vcf", alias, "/raw/1000Genomes/ALL.*.sites.vcf.gz", VCF, OverWrite)
    val annovar_dbnsfp =
      DatasetConf("annovar_dbnsfp", alias, "/raw/annovar/dbNSFP/hg38_dbnsfp41a.txt", CSV, OverWrite)
    val cancerhotspots_csv = DatasetConf(
      "cancerhotspots_csv",
      alias,
      "/raw/cancerhotspots/cancerhotspots.*.gz",
      CSV,
      OverWrite
    )
    val clinvar_vcf =
      DatasetConf("clinvar_vcf", alias, "/raw/clinvar/clinvar.vcf.gz", VCF, OverWrite)
    val cosmic_cancer_gene_census = DatasetConf(
      "cosmic_cancer_gene_census",
      alias,
      "/raw/cosmic/cancer_gene_census.csv",
      CSV,
      OverWrite
    )
    val dbsnp_vcf =
      DatasetConf("dbsnp_vcf", alias, "/raw/dbsnp/GCF_000001405.38.gz", VCF, OverWrite)
    val dbNSFP_csv = DatasetConf("dbNSFP_csv", alias, "/raw/dbNSFP/*.gz", CSV, OverWrite)
    val ddd_gene_census =
      DatasetConf("ddd_gene_census", alias, "/raw/ddd/DDG2P_17_5_2020.csv", CSV, OverWrite)
    val ensembl_canonical = DatasetConf(
      "ensembl_canonical",
      alias,
      "/raw/ensembl/*.GRCh38.104.canonical.tsv.gz",
      CSV,
      OverWrite
    )
    val ensembl_entrez = DatasetConf(
      "ensembl_entrez",
      alias,
      "/raw/ensembl/*.GRCh38.104.entrez.tsv.gz",
      CSV,
      OverWrite
    )
    val ensembl_ena =
      DatasetConf("ensembl_ena", alias, "/raw/ensembl/*.GRCh38.104.ena.tsv.gz", CSV, OverWrite)
    val ensembl_refseq = DatasetConf(
      "ensembl_refseq",
      alias,
      "/raw/ensembl/*.GRCh38.104.refseq.tsv.gz",
      CSV,
      OverWrite
    )
    val ensembl_uniprot = DatasetConf(
      "ensembl_uniprot",
      alias,
      "/raw/ensembl/*.GRCh38.104.uniprot.tsv.gz",
      CSV,
      OverWrite
    )
    val gnomad_genomes_3_1_1 = DatasetConf(
      "gnomad_genomes_3_1_1",
      alias,
      "/raw/gnomad/r3.1.1/gnomad.genomes.v3.1.1.sites.*.vcf.gz",
      VCF,
      OverWrite
    )
    val hpo_genes_to_phenotype = DatasetConf(
      "hpo_genes_to_phenotype",
      alias,
      "/raw/hpo/genes_to_phenotype.txt",
      CSV,
      OverWrite
    )
    val omim_genemap2 =
      DatasetConf("omim_genemap2", alias, "/raw/omim/genemap2.txt", CSV, OverWrite)
    val orphanet_gene_association =
      DatasetConf("en_product6", alias, "/raw/orphanet/en_product6.xml", XML, OverWrite)
    val orphanet_disease_history =
      DatasetConf("en_product9_ages", alias, "/raw/orphanet/en_product9_ages.xml", XML, OverWrite)
    val refseq_homo_sapiens_gene = DatasetConf(
      "refseq_homo_sapiens_gene",
      alias,
      "/raw/refseq/Homo_sapiens.gene_info.gz",
      CSV,
      OverWrite
    )
    val topmed_bravo_dbsnp =
      DatasetConf("topmed_bravo_dbsnp", alias, "/raw/topmed/bravo-dbsnp-all.vcf.gz", VCF, OverWrite)
    val all_participants = DatasetConf(
      "all_participants",
      alias,
      "/raw/participants/all_participants_*.json",
      JSON,
      OverWrite
    )

  }

  object Public extends StoreFolder {

    override val alias: String = kfStridesVariantBucket

    val `1000_genomes` = DatasetConf(
      "1000_genomes",
      alias,
      "/public/1000_genomes",
      PARQUET,
      OverWrite,
      TableConf("variant", "1000_genomes")
    )
    val cancer_hotspots = DatasetConf(
      "cancer_hotspots",
      alias,
      "/public/cancer_hotspots",
      PARQUET,
      OverWrite,
      TableConf("variant", "cancer_hotspots")
    )
    val clinvar = DatasetConf(
      "clinvar",
      alias,
      "/public/clinvar",
      PARQUET,
      OverWrite,
      TableConf("variant", "clinvar")
    )
    val cosmic_gene_set = DatasetConf(
      "cosmic_gene_set",
      alias,
      "/public/cosmic_gene_set",
      PARQUET,
      OverWrite,
      TableConf("variant", "cosmic_gene_set")
    )
    val dbnsfp_variant = DatasetConf(
      "dbnsfp",
      alias,
      "/public/dbnsfp/variant",
      PARQUET,
      OverWrite,
      Some(TableConf("variant", "dbnsfp")),
      partitionby = List("chromosome")
    )
    val dbnsfp_annovar = DatasetConf(
      "dbnsfp_annovar",
      alias,
      "/public/annovar/dbnsfp",
      PARQUET,
      OverWrite,
      Some(TableConf("variant", "dbnsfp_annovar")),
      partitionby = List("chromosome")
    )
    val dbnsfp_original = DatasetConf(
      "dbnsfp_original",
      alias,
      "/public/dbnsfp/scores",
      PARQUET,
      OverWrite,
      TableConf("variant", "dbnsfp_original")
    )
    val dbsnp = DatasetConf(
      "dbsnp",
      alias,
      "/public/dbsnp",
      PARQUET,
      OverWrite,
      Some(TableConf("variant", "dbsnp")),
      partitionby = List("chromosome")
    )
    val ddd_gene_set = DatasetConf(
      "ddd_gene_set",
      alias,
      "/public/ddd_gene_set",
      PARQUET,
      OverWrite,
      TableConf("variant", "ddd_gene_set")
    )
    val ensembl_mapping = DatasetConf(
      "ensembl_mapping",
      alias,
      "/public/ensembl_mapping",
      PARQUET,
      OverWrite,
      TableConf("variant", "ensembl_mapping")
    )
    val genes = DatasetConf(
      "genes",
      alias,
      "/public/genes",
      PARQUET,
      OverWrite,
      TableConf("variant", "genes")
    )
    val gnomad_genomes_2_1 = DatasetConf(
      "gnomad_genomes_2_1_1_liftover_grch38",
      alias,
      "/public/gnomad_genomes_2_1_1_liftover_grch38",
      PARQUET,
      OverWrite,
      TableConf("variant", "gnomad_genomes_2_1_1_liftover_grch38")
    )
    val gnomad_exomes_2_1 = DatasetConf(
      "gnomad_exomes_2_1_1_liftover_grch38",
      alias,
      "/public/gnomad_exomes_2_1_1_liftover_grch38",
      PARQUET,
      OverWrite,
      TableConf("variant", "gnomad_exomes_2_1_1_liftover_grch38")
    )
    val gnomad_genomes_3_0 = DatasetConf(
      "gnomad_genomes_3_0",
      alias,
      "/public/gnomad_genomes_3_0",
      PARQUET,
      OverWrite,
      TableConf("variant", "gnomad_genomes_3_0")
    )
    val gnomad_genomes_3_1_1 = DatasetConf(
      "gnomad_genomes_3_1_1",
      alias,
      "/public/gnomad_genomes_3_1_1",
      PARQUET,
      OverWrite,
      Some(TableConf("variant", "gnomad_genomes_3_1_1")),
      partitionby = List("chromosome")
    )
    val human_genes = DatasetConf(
      "human_genes",
      alias,
      "/public/human_genes",
      PARQUET,
      OverWrite,
      TableConf("variant", "human_genes")
    )
    val hpo_gene_set = DatasetConf(
      "hpo_gene_set",
      alias,
      "/public/hpo_gene_set",
      PARQUET,
      OverWrite,
      TableConf("variant", "hpo_gene_set")
    )
    val omim_gene_set = DatasetConf(
      "omim_gene_set",
      alias,
      "/public/omim_gene_set",
      PARQUET,
      OverWrite,
      TableConf("variant", "omim_gene_set")
    )
    val orphanet_gene_set = DatasetConf(
      "orphanet_gene_set",
      alias,
      "/public/orphanet_gene_set",
      PARQUET,
      OverWrite,
      TableConf("variant", "orphanet_gene_set")
    )
    val topmed_bravo = DatasetConf(
      "topmed_bravo",
      alias,
      "/public/topmed_bravo",
      PARQUET,
      OverWrite,
      TableConf("variant", "topmed_bravo")
    )
  }

  object DataService extends StoreFolder {

    override val alias: String = kfStridesVariantBucket

    val studies = DatasetConf(
      "studies",
      alias,
      "/dataservice/studies/studies_re_*",
      PARQUET,
      OverWrite,
      TableConf("variant", "studies")
    )
    val biospecimens = DatasetConf(
      "biospecimens",
      alias,
      "/dataservice/biospecimens/biospecimens_re_*",
      PARQUET,
      OverWrite,
      TableConf("variant", "biospecimens")
    )
    val family_relationships = DatasetConf(
      "family_relationships",
      alias,
      "/dataservice/family_relationships/family_relationships_re_*",
      PARQUET,
      OverWrite,
      TableConf("variant", "family_relationships")
    )
    val participants = DatasetConf(
      "participants",
      alias,
      "/dataservice/participants/participants_re_*",
      PARQUET,
      OverWrite,
      TableConf("variant", "participants")
    )
    val genomic_files = DatasetConf(
      "genomic_files",
      alias,
      "/dataservice/genomic_files/genomic_files_re_*",
      PARQUET,
      OverWrite,
      TableConf("variant", "genomic_files")
    )
    val genomic_files_override = DatasetConf(
      "genomic_files_override",
      alias,
      "/genomic_files_override",
      CSV,
      OverWrite,
      TableConf("variant", "genomic_files_override")
    )

  }

  object Clinical extends StoreFolder {

    override val alias: String = kfStridesVariantBucket

    val consequences = DatasetConf(
      "consequences",
      alias,
      "/consequences/consequences_re_*",
      PARQUET,
      OverWrite,
      TableConf("variant", "consequences")
    )
    val occurrences = DatasetConf(
      "occurrences",
      alias,
      "/occurrences/occurrences_sd_*_re_*",
      PARQUET,
      OverWrite,
      TableConf("variant", "occurrences")
    )
    val occurrences_family = DatasetConf(
      "occurrences_family",
      alias,
      "/occurrences/occurrences_family_sd_*_re_*",
      PARQUET,
      OverWrite,
      TableConf("variant", "occurrences_family")
    )
    val variants = DatasetConf(
      "variants",
      alias,
      "/variants/variants_re_*",
      PARQUET,
      OverWrite,
      TableConf("variant", "variants")
    )
  }

  object Es extends StoreFolder {

    override val alias: String = kfStridesVariantBucket
    val relativePath           = s"/es_index"
    val variant_centric = DatasetConf(
      "variant_centric",
      alias,
      s"$relativePath/variant_centric",
      PARQUET,
      OverWrite,
      Some(TableConf("portal", "variant_centric")),
      partitionby = List("chromosome")
    )
    val gene_centric = DatasetConf(
      "gene_centric",
      alias,
      s"$relativePath/gene_centric",
      PARQUET,
      OverWrite,
      TableConf("portal", "gene_centric")
    )
    val genomic_suggestions = DatasetConf(
      "genomic_suggestions",
      alias,
      s"$relativePath/genomic_suggestions",
      PARQUET,
      OverWrite,
      TableConf("portal", "genomic_suggestions")
    )
  }

  def sources: Set[DatasetConf] = Raw.sources ++ Public.sources ++ Clinical.sources ++ Es.sources

}
