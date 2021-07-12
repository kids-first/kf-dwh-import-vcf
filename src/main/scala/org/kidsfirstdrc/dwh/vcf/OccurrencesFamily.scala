package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.spark3.config.Configuration
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, DataService, HarmonizedData}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

class OccurrencesFamily(studyId: String,
                        releaseId: String,
                        input: String,
                        biospecimenIdColumn: String,
                        cgpPattern: String,
                        postCgpPattern: String,
                        referenceGenomePath: Option[String] = None
)(implicit conf: Configuration)
    extends ETL() {

  val destination = Clinical.occurrences_family

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    val inputDF: DataFrame = unionCGPFiles(input, studyId, releaseId, cgpPattern, postCgpPattern, referenceGenomePath)

    val biospecimens =
      spark.table(s"${DataService.biospecimens.table.get.fullName}_${releaseId.toLowerCase}")
    val participants =
      spark.table(s"${DataService.participants.table.get.fullName}_${releaseId.toLowerCase}")
    val family_relationships = spark.table(
      s"${DataService.family_relationships.table.get.fullName}_${releaseId.toLowerCase}"
    )

    Map(
      DataService.participants.id           -> participants,
      DataService.biospecimens.id           -> biospecimens,
      DataService.family_relationships.id   -> family_relationships,
      HarmonizedData.family_variants_vcf.id -> inputDF
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val participants = data(DataService.participants.id)
      .withColumn(
        "affected_status",
        when(col("affected_status").cast(StringType) === "true", lit(true))
          .otherwise(
            when(col("affected_status") === "affected", lit(true))
              .otherwise(lit(false))
          )
      )
      .select("kf_id", "is_proband", "affected_status")

    val biospecimens = data(DataService.biospecimens.id)
      .select(
        col(biospecimenIdColumn).as("joined_sample_id"),
        $"biospecimen_id",
        $"participant_id",
        $"family_id",
        coalesce($"dbgap_consent_code", lit("_NONE_")) as "dbgap_consent_code",
        ($"consent_type" === "GRU") as "is_gru",
        ($"consent_type" === "HMB") as "is_hmb"
      )
      .drop("joined_sample_id")

    val family_relationships = data(DataService.family_relationships.id)
      .where($"participant1_to_participant2_relation".isin("Mother", "Father"))

    val family_variants_vcf = data(HarmonizedData.family_variants_vcf.id)

    val occurrences = selectOccurrences(studyId, releaseId, family_variants_vcf)

    val joinedBiospecimen =
      biospecimens
        .join(participants, biospecimens("participant_id") === participants("kf_id"))
        .select(biospecimens("*"), $"is_proband", $"affected_status")

    val relations =
      family_relationships
        .groupBy("participant2")
        .agg(
          map_from_entries(
            collect_list(
              struct(
                $"participant1_to_participant2_relation" as "relation",
                $"participant1" as "participant_id"
              )
            )
          ) as "relations"
        )
        .select(
          $"participant2" as "participant_id",
          $"relations.Mother" as "mother_id",
          $"relations.Father" as "father_id"
        )

    val joinedRelation = joinedBiospecimen.join(relations, Seq("participant_id"), "left")

    occurrences
      .join(broadcast(joinedRelation), Seq("biospecimen_id"))
      .withColumn(
        "family_calls",
        when($"family_id".isNotNull, familyCalls).otherwise(
          lit(null).cast(MapType(StringType, ArrayType(IntegerType)))
        )
      )
      .withColumn(
        "mother_calls",
        when($"family_id".isNotNull, $"family_calls" ($"mother_id")).otherwise(
          lit(null).cast(ArrayType(IntegerType))
        )
      )
      .withColumn(
        "father_calls",
        when($"family_id".isNotNull, $"family_calls" ($"father_id")).otherwise(
          lit(null).cast(ArrayType(IntegerType))
        )
      )
      .drop("family_calls")
      .withColumn("zygosity", zygosity($"calls"))
      .withColumn("mother_zygosity", zygosity($"mother_calls"))
      .withColumn("father_zygosity", zygosity($"father_calls"))
      .withParentalOrigin("parental_origin", $"father_calls", $"mother_calls")
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val tableOccurence = tableName(destination.id, studyId, releaseId)
    data
      .repartitionByRange(700, $"dbgap_consent_code", $"family_id", $"participant_id")
      .write
      .mode("overwrite")
      .partitionBy("study_id", "dbgap_consent_code", "family_id", "participant_id")
      .format("parquet")
      .option("path", s"${destination.rootPath}/${destination.id}/$tableOccurence")
      .saveAsTable(tableOccurence)

    data
  }

  override def run()(implicit spark: SparkSession): DataFrame = {
    val outputDf = transform(extract())
    load(outputDf)
  }

  def selectOccurrences(studyId: String, releaseId: String, inputDF: DataFrame)(implicit
      spark: SparkSession
  ): DataFrame = {
    import spark.implicits._

    val occurrences = inputDF
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        $"hgvsg",
        $"variant_class",
        $"genotype.sampleId" as "biospecimen_id",
        $"genotype.alleleDepths" as "ad",
        $"genotype.depth" as "dp",
        $"genotype.conditionalQuality" as "gq",
        $"genotype.calls" as "calls",
        has_alt,
        is_multi_allelic,
        old_multi_allelic,
        $"qual" as "quality",
        $"INFO_filters" (0) as "filter",
        ac as "info_ac",
        an as "info_an",
        $"INFO_AF" (0) as "info_af",
        $"INFO_culprit" as "info_culprit",
        $"INFO_SOR" as "info_sor",
        $"INFO_ReadPosRankSum" as "info_read_pos_rank_sum",
        $"INFO_InbreedingCoeff" as "info_inbreeding_coeff",
        $"INFO_PG" as "info_pg",
        $"INFO_FS" as "info_fs",
        $"INFO_DP" as "info_dp",
        optional_info(inputDF, "INFO_DS", "info_ds", "boolean"),
        $"INFO_NEGATIVE_TRAIN_SITE" as "info_info_negative_train_site",
        $"INFO_POSITIVE_TRAIN_SITE" as "info_positive_train_site",
        $"INFO_VQSLOD" as "info_vqslod",
        $"INFO_ClippingRankSum" as "info_clipping_rank_sum",
        $"INFO_RAW_MQ" as "info_raw_mq",
        $"INFO_BaseQRankSum" as "info_base_qrank_sum",
        $"INFO_MLEAF" (0) as "info_mleaf",
        $"INFO_MLEAC" (0) as "info_mleac",
        $"INFO_MQ" as "info_mq",
        $"INFO_QD" as "info_qd",
        $"INFO_DB" as "info_db",
        $"INFO_MQRankSum" as "info_m_qrank_sum",
        optional_info(inputDF, "INFO_loConfDeNovo", "lo_conf_denovo"),
        optional_info(inputDF, "INFO_hiConfDeNovo", "hi_conf_denovo"),
        $"INFO_ExcessHet" as "info_excess_het",
        optional_info(inputDF, "INFO_HaplotypeScore", "info_haplotype_score", "float"),
        $"file_name",
        lit(studyId) as "study_id",
        lit(releaseId) as "release_id",
        is_normalized
      )
      .withColumn(
        "is_lo_conf_denovo",
        array_contains(split($"lo_conf_denovo", ","), $"biospecimen_id")
      )
      .withColumn(
        "is_hi_conf_denovo",
        array_contains(split($"hi_conf_denovo", ","), $"biospecimen_id")
      )
      .drop("annotation", "lo_conf_denovo", "hi_conf_denovo")
    occurrences
  }

  /** This function is a hack for loading both CGP and postCGP files.
    * These 2 kinds of vcf does not have the same headers, so it results in error when trying to parse these files together
    * That's why we need to address schema differences, and then union these 2 dtafremaes manually.
    *
    * @param input path where are located the files
    * @param studyId
    * @param releaseId
    * @param spark
    * @return a dataframe that unions cgp and postcgp vcf
    */
  private def unionCGPFiles(input: String,
                            studyId: String,
                            releaseId: String,
                            cgpPattern: String,
                            postCgpPattern: String,
                            referenceGenomePath: Option[String] = None
  )(implicit spark: SparkSession): DataFrame = {
    val postCGPFiles = getVisibleFiles(input, studyId, releaseId, postCgpPattern)
    val CGPFiles     = getVisibleFiles(input, studyId, releaseId, cgpPattern)
    val vcfDf = (postCGPFiles, CGPFiles) match {
      case (Nil, Nil)                                   => throw new IllegalStateException("No VCF files found!")
      case (Nil, genomicFiles) if genomicFiles.nonEmpty => asCGP(vcf(genomicFiles, referenceGenomePath))
      case (genomicFiles, Nil) if genomicFiles.nonEmpty => asPostCGP(vcf(genomicFiles, referenceGenomePath))
      case (postCGPFiles, cgpFiles) =>
        val postCGP = asPostCGP(vcf(postCGPFiles, referenceGenomePath))
        val cgp     = asCGP(vcf(cgpFiles, referenceGenomePath))
        union(postCGP, cgp)

    }

    vcfDf.withColumn("file_name", filename)
  }

  private def asPostCGP(inputDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    inputDf
      .withColumn("annotation", annotations)
      .withColumn("hgvsg", array_sort(col("annotation.HGVSg"))(0))
      .withColumn("variant_class", array_sort(col("annotation.VARIANT_CLASS"))(0))
      .drop("annotation", "INFO_ANN")
      .withColumn("genotype", explode(col("genotypes")))
  }

  private def asCGP(inputDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    inputDf
      .withColumn("annotation", annotations)
      .withColumn("hgvsg", array_sort(col("annotation.HGVSg"))(0))
      .withColumn("variant_class", array_sort(col("annotation.VARIANT_CLASS"))(0))
      .drop("annotation", "INFO_ANN")
      .withColumn("INFO_DS", lit(null).cast("boolean"))
      .withColumn("INFO_HaplotypeScore", lit(null).cast("double"))
      .withColumn("genotype", explode($"genotypes"))
      .drop("genotypes")
      .withColumn("INFO_ReadPosRankSum", $"INFO_ReadPosRankSum" (0))
      .withColumn("INFO_ClippingRankSum", $"INFO_ClippingRankSum" (0))
      .withColumn("INFO_RAW_MQ", $"INFO_RAW_MQ" (0))
      .withColumn("INFO_BaseQRankSum", $"INFO_BaseQRankSum" (0))
      .withColumn("INFO_MQRankSum", $"INFO_MQRankSum" (0))
      .withColumn("INFO_ExcessHet", $"INFO_ExcessHet" (0))
      .withColumn(
        "genotype",
        struct(
          $"genotype.sampleId",
          $"genotype.conditionalQuality",
          $"genotype.filters",
          $"genotype.SB",
          $"genotype.alleleDepths",
          $"genotype.PP",
          $"genotype.PID" (0) as "PID",
          $"genotype.phased",
          $"genotype.calls",
          $"genotype.MIN_DP" (0) as "MIN_DP",
          $"genotype.JL",
          $"genotype.PGT" (0) as "PGT",
          $"genotype.phredLikelihoods",
          $"genotype.depth",
          $"genotype.RGQ",
          $"genotype.JP"
        )
      )
  }

  def joinOccurrencesWithInheritance(occurrences: DataFrame, relations: DataFrame)(implicit
      spark: SparkSession
  ): DataFrame = {
    import spark.implicits._
    occurrences
      .join(relations, occurrences("participant_id") === relations("participant_id"), "left")
      .drop(relations("participant_id"))
      .withColumn(
        "family_calls",
        when($"family_id".isNotNull, familyCalls).otherwise(
          lit(null).cast(MapType(StringType, ArrayType(IntegerType)))
        )
      )
      .withColumn(
        "mother_calls",
        when($"family_id".isNotNull, $"family_calls" ($"mother_id")).otherwise(
          lit(null).cast(ArrayType(IntegerType))
        )
      )
      .withColumn(
        "father_calls",
        when($"family_id".isNotNull, $"family_calls" ($"father_id")).otherwise(
          lit(null).cast(ArrayType(IntegerType))
        )
      )
      .drop("family_calls")
      .withColumn("zygosity", zygosity($"calls"))
      .withColumn("mother_zygosity", zygosity($"mother_calls"))
      .withColumn("father_zygosity", zygosity($"father_calls"))
      .withParentalOrigin("parental_origin", $"father_calls", $"mother_calls")
  }
}
