package org.kidsfirstdrc.dwh.testutils.vcf

case class ConsequenceInput(
    Allele: String = "A",
    Consequence: Seq[String] = Seq("missense_variant"),
    SYMBOL: String = "SCN2A",
    Gene: String = "ENSG00000136531",
    Feature: String = "ENST00000283256.10",
    IMPACT: String = "MODERATE",
    BIOTYPE: String = "protein_coding",
    Feature_type: String = "Transcript",
    STRAND: Int = 1,
    VARIANT_CLASS: String = "SNV",
    HGVSg: String = "chr2:g.166166916G>A",
    HGVSc: String = "ENST00000283256.10:c.781G>A",
    HGVSp: String = "ENSP00000283256.6:p.Val261Met",
    CANONICAL: String = "YES",
    cDNA_position: Option[Int] = Some(937),
    CDS_position: Option[Int] = Some(781),
    Amino_acids: Option[RefAlt] = Some(RefAlt("V", "M")),
    Protein_position: Option[Int] = Some(261),
    Codons: Option[RefAlt] = Some(RefAlt("GTG", "ATG")),
    EXON: Option[Exon] = Some(Exon(Some(7), Some(27))),
    INTRON: Option[Intron] = None
)
