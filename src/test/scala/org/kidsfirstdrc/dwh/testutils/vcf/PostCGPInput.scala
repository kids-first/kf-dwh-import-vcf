package org.kidsfirstdrc.dwh.testutils.vcf

case class PostCGPInput(`contigName`: String = "chr2",
                        `start`: Long = 165310406,
                        `end`: Long = 165310406,
                        `names`: List[String] = List(),
                        `referenceAllele`: String = "G",
                        `alternateAlleles`: List[String] = List("A"),
                        `qual`: Double = 39.48,
                        `INFO_FILTERS`: List[String] = List("VQSRTrancheINDEL99.90to99.95"),
                        `splitFromMultiAllelic`: Boolean = false,
                        `INFO_loConfDeNovo`: Option[String] = None,
                        `INFO_NEGATIVE_TRAIN_SITE`: Boolean = true,
                        `INFO_AC`: List[Int] = List(1),
                        `INFO_culprit`: String = "DP",
                        `INFO_SOR`: Double = 0.732,
                        `INFO_ReadPosRankSum`: Double = 2.69,
                        `INFO_AN`: Int = 6,
                        `INFO_InbreedingCoeff`: Option[Double] = None,
                        `INFO_PG`: List[Int] = List(0, 0, 0),
                        `INFO_AF`: List[Double] = List(0.167),
                        `INFO_FS`: Double = 0.0,
                        `INFO_DP`: Int = 334,
                        `INFO_POSITIVE_TRAIN_SITE`: Option[Boolean] = None,
                        `INFO_VQSLOD`: Double = -7.719,
                        `INFO_ClippingRankSum`: Double = 0.228,
                        `INFO_RAW_MQ`: Double = 46212.0,
                        `INFO_ANN`: List[Info_Ann] = List(Info_Ann(`HGVSg` = null), Info_Ann(`VARIANT_CLASS` = null)),
                        `INFO_BaseQRankSum`: Double = -0.959,
                        `INFO_MLEAF`: List[Double] = List(0.167),
                        `INFO_MLEAC`: List[Int] = List(1),
                        `INFO_MQ`: Double = 36.34,
                        `INFO_QD`: Double = 1.72,
                        `INFO_END`: Option[Int] = None,
                        `INFO_DB`: Option[Boolean] = None,
                        `INFO_HaplotypeScore`: Option[Double] = None,
                        `INFO_MQRankSum`: Double = -0.502,
                        `INFO_hiConfDeNovo`: Option[String] = None,
                        `INFO_ExcessHet`: Double = 3.6798,
                        `INFO_DS`: Option[Boolean] = None,
                        `INFO_OLD_MULTIALLELIC`: Option[String] = None,
                        `genotypes`: List[GENOTYPES] = List(GENOTYPES(), GENOTYPES(`sampleId` = "BS_HIJKKL")))


case class Info_Ann(`Allele`: String = "T",
                    `Consequence`: List[String] = List("downstream_gene_variant"),
                    `IMPACT`: String = "MODIFIER",
                    `SYMBOL`: String = "PF4",
                    `Gene`: String = "ENSG00000163737",
                    `Feature_type`: String = "Transcript",
                    `Feature`: String = "ENST00000296029",
                    `BIOTYPE`: String = "protein_coding",
                    `EXON`: Exon = Exon(),
                    `INTRON`: Intron = Intron(),
                    `HGVSc`: Option[String] = None,
                    `HGVSp`: Option[String] = None,
                    `cDNA_position`: Option[Int] = None,
                    `CDS_position`: Option[Int] = None,
                    `Protein_position`: Option[Int] = None,
                    `Amino_acids`: AMINO_ACIDS = AMINO_ACIDS(),
                    `Codons`: CODONS = CODONS(),
                    `Existing_variation`: Option[List[String]] = None,
                    `DISTANCE`: Int = 1640,
                    `STRAND`: Int = -1,
                    `FLAGS`: Option[List[String]] = None,
                    `VARIANT_CLASS`: String = "SNV",
                    `SYMBOL_SOURCE`: String = "HGNC",
                    `HGNC_ID`: String = "HGNC:8861",
                    `CANONICAL`: String = "YES",
                    `SIFT`: Option[String] = None,
                    `HGVS_OFFSET`: Option[String] = None,
                    `HGVSg`: String = "chr4:g.73979437G>T")


case class GENOTYPES(`sampleId`: String = "BS_HIJKKL2",
                     `conditionalQuality`: Int = 99,
                     `filters`: Option[List[String]] = None,
                     `SB`: Option[List[Int]] = None,
                     `alleleDepths`: List[Int] = List(34, 0),
                     `PP`: List[Int] = List(0, 99, 1066),
                     `PID`: Option[String] = None,
                     `phased`: Boolean = false,
                     `calls`: List[Int] = List(0, 0),
                     `MIN_DP`: Option[Int] = None,
                     `JL`: Int = 72,
                     `PGT`: Option[String] = None,
                     `phredLikelihoods`: List[Int] = List(0, 99, 1066),
                     `depth`: Int = 34,
                     `RGQ`: Option[Int] = None,
                     `JP`: Int = 98)


case class AMINO_ACIDS(`reference`: Option[String] = None,
                       `variant`: Option[String] = None)


case class Exon(`rank`: Option[Int] = None,
                `total`: Option[Int] = None)


case class Intron(`rank`: Int = 2,
                  `total`: Int = 10)


case class CODONS(`reference`: Option[String] = None,
                  `variant`: Option[String] = None)
