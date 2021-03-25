package org.kidsfirstdrc.dwh.testutils.vcf

case class ConsequenceOutput(chromosome: String = "2",
                             start: Long = 165310406,
                             end: Long = 165310406,
                             reference: String = "G",
                             alternate: String = "A",
                             symbol: String = "SCN2A",
                             impact: String = "MODERATE",
                             ensembl_gene_id: String = "ENSG00000136531",
                             ensembl_transcript_id: Option[String] = Some("ENST00000283256.10"),
                             ensembl_regulatory_id: Option[String] = None,
                             feature_type: String = "Transcript",
                             consequences: Seq[String] = Seq("missense_variant"),
                             biotype: Option[String] = Some("protein_coding"),
                             name: Option[String] = Some("rs1057520413"),
                             variant_class: String = "SNV",
                             strand: Int = 1,
                             hgvsg: Option[String] = Some("chr2:g.166166916G>A"),
                             hgvsc: Option[String] = Some("ENST00000283256.10:c.781G>A"),
                             hgvsp: Option[String] = Some("ENSP00000283256.6:p.Val261Met"),
                             exon: Option[Exon] = Some(Exon(Some(7), Some(27))),
                             intron: Option[Intron] = None,
                             cdna_position: Option[Int] = Some(937),
                             cds_position: Option[Int] = Some(781),
                             amino_acids: Option[RefAlt] = Some(RefAlt("V", "M")),
                             codons: Option[RefAlt] = Some(RefAlt("GTG", "ATG")),
                             protein_position: Option[Int] = Some(261),
                             canonical: Boolean = true,
                             study_id: String = "SD_123456",
                             release_id: String = "RE_ABCDEF")

//case class Exon(rank: Int, total: Int)

//case class Intron(rank: Int, total: Int)

case class RefAlt(reference: String, variant: String)
