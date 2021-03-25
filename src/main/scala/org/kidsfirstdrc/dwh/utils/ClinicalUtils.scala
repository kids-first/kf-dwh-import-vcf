package org.kidsfirstdrc.dwh.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, SparkSession}

object ClinicalUtils {

  implicit class DataFrameOps(df: DataFrame) {

    def joinAndMerge(other: DataFrame, outputColumnName: String, joinType: String = "inner"): DataFrame = {
      val otherFields = other.drop("chromosome", "start", "end", "name", "reference", "alternate")
      df.joinByLocus(other, joinType)
        .withColumn(outputColumnName, when(col(otherFields.columns.head).isNotNull, struct(otherFields("*"))).otherwise(lit(null)))
        .select(df.columns.map(col) :+ col(outputColumnName): _*)
    }

    def joinByLocus(other: DataFrame, joinType: String): DataFrame = {
      df.join(other, Seq("chromosome", "start", "reference", "alternate"), joinType)
    }

    def groupByLocus(): RelationalGroupedDataset = {
      df.groupBy(col("chromosome"), col("start"), col("reference"), col("alternate"))
    }

    def selectLocus(cols: Column*): DataFrame = {
      val allCols = col("chromosome") :: col("start") :: col("reference") :: col("alternate") :: cols.toList
      df.select(allCols: _*)
    }

  }

  private def loadClinicalTable(studyId: String, releaseId: String, tableName: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark
      .read
      //.parquet(s"s3a://kf-strides-variant-parquet-prd/dataservice/$tableName/${tableName}_${releaseId.toLowerCase}")
      .table(s"${tableName}_${releaseId.toLowerCase}")
      .where($"study_id" === studyId)
  }

  def getRelations(studyId: String, releaseId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    broadcast(
      loadClinicalTable(studyId, releaseId, "family_relationships")
        .where($"participant1_to_participant2_relation" isin("Mother", "Father"))
        .groupBy("participant2")
        .agg(
          map_from_entries(
            collect_list(
              struct($"participant1_to_participant2_relation" as "relation", $"participant1" as "participant_id")
            )
          ) as "relations"
        )
        .select($"participant2" as "participant_id", $"relations.Mother" as "mother_id", $"relations.Father" as "father_id")

    )

  }


  def getBiospecimens(studyId: String, releaseId: String, biospecimenIdColumn: String)(implicit spark: SparkSession): DataFrame = {
    val biospecimen_id_col = col(biospecimenIdColumn).as("joined_sample_id")
    import spark.implicits._

    val b = loadClinicalTable(studyId, releaseId, "biospecimens")
      .select(biospecimen_id_col, $"biospecimen_id", $"participant_id", $"family_id",
        coalesce($"dbgap_consent_code", lit("_NONE_")) as "dbgap_consent_code",
        ($"consent_type" === "GRU") as "is_gru",
        ($"consent_type" === "HMB") as "is_hmb"
      )
      .alias("b")

    val p = loadClinicalTable(studyId, releaseId, "participants")
      .withColumn("affected_status",
        when(col("affected_status").cast(StringType) === "true", lit(true))
          .otherwise(when(col("affected_status") === "affected", lit(true))
            .otherwise(lit(false))))
      .select("kf_id", "is_proband", "affected_status").alias("p")
    val all = b.join(p, b("participant_id") === p("kf_id")).select("b.*", "p.is_proband", "p.affected_status")

    broadcast(all)
  }

  def loadManifestFile(studyId: String)(implicit spark: SparkSession): DataFrame = {
    //actual path => "s3://kf-strides-variant-parquet-prd/genomic_files_override/"
    spark
      .table("variant.genomic_files_override")
      .filter(col("study_id").isin(studyId))
      .select("file_name")
  }


  def getGenomicFiles(studyId: String, releaseId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    broadcast(
      loadClinicalTable(studyId, releaseId, "genomic_files")
        .select($"file_name")
        .unionByName(loadManifestFile(studyId))
        .dropDuplicates("file_name")
    )
  }

  val filterAcl: String => UserDefinedFunction = studyId => udf { (acl: Seq[String]) =>
    if (acl == null) None else {
      Some(acl.filter(a => (a != studyId && !a.endsWith(".c999")) || a == "*"))
    }
  }
}
