package org.kidsfirstdrc.dwh.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ClinicalUtils {

  private def loadClinicalTable(studyId: String, releaseId: String, tableName: String)(implicit spark: SparkSession) = {
    import spark.implicits._
    spark
      .table(s"${tableName}_${releaseId.toLowerCase}")
      .where($"study_id" === studyId)
  }

  def getRelations(studyId: String, releaseId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    broadcast(
      loadClinicalTable(studyId, releaseId, "family_relationships")
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
      .select(biospecimen_id_col, $"biospecimen_id", $"participant_id", $"family_id", $"dbgap_consent_code",
        array_contains($"duo_ids", "DUO:0000042") as "is_gru",
        array_contains($"duo_ids", "DUO:0000006") as "is_hmb"
      )
      .where($"dbgap_consent_code".isNotNull)
      .alias("b")

    val p = loadClinicalTable(studyId, releaseId, "participants").select("kf_id", "is_proband", "affected_status").alias("p")
    val all = b.join(p, b("participant_id") === p("kf_id")).select("b.*", "p.is_proband", "p.affected_status")

    broadcast(all)
  }


  def getGenomicFiles(studyId: String, releaseId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    broadcast(
      loadClinicalTable(studyId, releaseId, "genomic_files")
        .select($"file_name", $"acl")
        .withColumn("acl", filterAcl(studyId)($"acl")).where(size($"acl") <= 1).select($"acl"(0) as "acl", $"file_name")
        .select(when($"acl".isNull, "_NONE_").when($"acl" === "*", "_PUBLIC_").otherwise($"acl") as "dbgap_consent_code", $"file_name")
    )
  }

  val filterAcl: String => UserDefinedFunction = studyId => udf { (acl: Seq[String]) =>
    if (acl == null) None else {
      Some(acl.filter(a => (a != studyId && !a.endsWith(".c999")) || a == "*"))
    }
  }
}
