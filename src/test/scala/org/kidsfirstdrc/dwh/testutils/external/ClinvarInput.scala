/**
 * Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
 * on 2021-02-02T08:00:17.290
 */
package org.kidsfirstdrc.dwh.testutils.external


case class ClinvarInput(contigName: String = "2",
                        start: Long = 69359260,
                        end: Long = 69359261,
                        names: List[String] = List("257668"),
                        referenceAllele: String = "T",
                        alternateAlleles: List[String] = List("A"),
                        qual: Option[Double] = None,
                        filters: List[String] = List(""),
                        splitFromMultiAllelic: Boolean = false,
                        INFO_AF_EXAC: Double = 0.00394,
                        INFO_CLNVCSO: String = "SO:0001483",
                        INFO_GENEINFO: String = "GFPT1:2673",
                        INFO_CLNSIGINCL: Option[List[String]] = None,
                        INFO_CLNVI: List[String] = List("Illumina_Clinical_Services_Laboratory", "Illumina:747283"),
                        INFO_CLNDISDB: List[String] = List("MONDO:MONDO:0012518", "MedGen:C3552335", "OMIM:610542|MedGen:CN169374|MedGen:CN517202"),
                        INFO_CLNREVSTAT: List[String] = List("criteria_provided", "_multiple_submitters", "_no_conflicts"),
                        INFO_CLNDN: List[String] = List("Congenital_myasthenic_syndrome_12|not_specified|not_provided"),
                        INFO_ALLELEID: Int = 250763,
                        INFO_ORIGIN: List[String] = List("1"),
                        INFO_SSR: Option[Int] = None,
                        INFO_CLNDNINCL: Option[List[String]] = None,
                        INFO_CLNSIG: List[String] = List("Benign"),
                        INFO_RS: List[String] = List("112682152"),
                        INFO_DBVARID: Option[List[String]] = None,
                        INFO_AF_TGP: Double = 0.01118,
                        INFO_CLNVC: String = "single_nucleotide_variant",
                        INFO_CLNHGVS: List[String] = List("NC_000002.12:g.69359261T>A"),
                        INFO_MC: List[String] = List("SO:0001627|intron_variant"),
                        INFO_CLNSIGCONF: Option[List[String]] = None,
                        INFO_AF_ESP: Double = 0.01415,
                        INFO_CLNDISDBINCL: Option[List[String]] = None,
                        genotypes: List[GENOTYPES] = List(GENOTYPES()) )


case class GENOTYPES(sampleId: String = "id")

