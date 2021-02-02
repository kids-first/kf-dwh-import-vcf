package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.utils.Environment.Environment
import org.kidsfirstdrc.dwh.utils.{Environment, MultiSourceEtlJob}

import scala.xml.{Elem, Node, XML}

class ImportOrphanetJob(runEnv: Environment) extends MultiSourceEtlJob(runEnv) {

  override val database = "variant"
  override val tableName = "orphanet_gene_set"

  override def extract(input: String)(implicit spark: SparkSession): Map[String, DataFrame] = {
    import spark.implicits._

    def loadXML: String => Elem = str => XML.loadString(spark.read.text(str).collect().map(_.getString(0)).mkString("\n"))

    Map(
      "gene_association" -> parseProduct6XML(loadXML(s"$input/en_product6.xml")).toDF,
      "disease_history" -> parseProduct9XML(loadXML(s"$input/en_product9_ages.xml")).toDF
    )

  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    data("gene_association")
      .join(
        data("disease_history").select("orpha_code", "average_age_of_onset", "average_age_of_death","type_of_inheritance"),
        Seq("orpha_code"), "left")
  }

  override def load(data: DataFrame, output: String)(implicit spark: SparkSession): DataFrame = {
    data
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("parquet")
      .option("path", s"$output/$tableName")
      .saveAsTable(s"$database.$tableName")

    if (runEnv == Environment.PROD)
      spark.sql(s"create or replace view variant_live.$tableName as select * from $database.$tableName")
    data
  }

  private def getIdFromSourceName: (Node, String) => Option[String] =
    (genes, name) => (genes \ "ExternalReferenceList" \ "ExternalReference").find(node => (node \ "Source").text == name).map(_ \ "Reference").map(_.text)

  val ensembl_gene_id:    Node => Option[String] = genes => getIdFromSourceName(genes, "Ensembl")
  val genatlas_gene_id:   Node => Option[String] = genes => getIdFromSourceName(genes, "Genatlas")
  val HGNC_gene_id:       Node => Option[String] = genes => getIdFromSourceName(genes, "HGNC")
  val omim_gene_id:       Node => Option[String] = genes => getIdFromSourceName(genes, "OMIM")
  val reactome_gene_id:   Node => Option[String] = genes => getIdFromSourceName(genes, "Reactome")
  val swiss_prot_gene_id: Node => Option[String] = genes => getIdFromSourceName(genes, "SwissProt")

  val association_type: Node => Option[String] =
    geneAssociation => (geneAssociation \ "DisorderGeneAssociationType" \ "Name").headOption.map(_.text)

  val association_type_id: Node => Option[Long] =
    geneAssociation => (geneAssociation \ "DisorderGeneAssociationType").headOption.flatMap(_.attribute("id")).map(_.text.toLong)

  val association_status: Node => Option[String] =
    geneAssociation => (geneAssociation \ "DisorderGeneAssociationStatus" \ "Name").headOption.map(_.text)

  def parseProduct6XML(doc: Elem): Seq[OrphanetGeneAssociation] = {
    for {
      disorder <- doc \\ "DisorderList" \\ "Disorder"
      orphaNumber <- disorder \ "OrphaCode"
      expertLink <- disorder \ "ExpertLink"
      name <- disorder \ "Name"
      disorderType <- disorder \ "DisorderType"
      disorderTypeName <- disorderType \ "Name"
      disorderGroup <- disorder \ "DisorderGroup"
      disorderGroupName <- disorderGroup \ "Name"
      geneAssociation <- disorder \ "DisorderGeneAssociationList" \ "DisorderGeneAssociation"
      genes <- geneAssociation \ "Gene"
      locus <- genes \ "LocusList" \ "Locus"
    } yield {
      OrphanetGeneAssociation(
        disorder.attribute("id").get.text.toLong,
        orphaNumber.text.toLong,
        expertLink.text,
        name.text,
        disorderType.attribute("id").get.text.toLong,
        disorderTypeName.text,
        disorderGroup.attribute("id").get.text.toLong,
        disorderGroupName.text,
        (geneAssociation \ "SourceOfValidation").text,
        genes.attribute("id").get.text.toLong,
        (genes \ "Symbol").text,
        (genes \ "Name").text,
        (genes \ "SynonymList" \\ "Synonym").map(_.text).toList,
        ensembl_gene_id(genes),
        genatlas_gene_id(genes),
        HGNC_gene_id(genes),
        omim_gene_id(genes),
        reactome_gene_id(genes),
        swiss_prot_gene_id(genes),
        association_type(geneAssociation),
        association_type_id(geneAssociation),
        association_status(geneAssociation),
        locus.attribute("id").get.text.toLong,
        (genes \ "LocusList" \ "Locus" \ "GeneLocus").text,
        (genes \ "LocusList" \ "Locus" \ "LocusKey").text.toLong
      )
    }
  }

  def parseProduct9XML(doc: Elem): Seq[OrphanetDiseaseHistory] = {
    for {
      disorder <- doc \\ "DisorderList" \\ "Disorder"
      orphaCode <- disorder \ "OrphaCode"
      expertLink <- disorder \ "ExpertLink"
      name <- disorder \ "Name"
      disorderType <- disorder \ "DisorderType"
      disorderTypeName <- disorderType \ "Name"
      disorderGroup <- disorder \ "DisorderGroup"
      disorderGroupName <- disorderType \ "Name"
    } yield {
      OrphanetDiseaseHistory(
        disorder.attribute("id").get.text.toLong,
        orphaCode.text.toLong,
        expertLink.text,
        name.text,
        disorderType.attribute("id").get.text.toLong,
        disorderTypeName.text,
        disorderGroup.attribute("id").get.text.toLong,
        disorderGroupName.text,
        (disorder \\ "AverageAgeOfOnsetList" \ "AverageAgeOfOnset" \ "Name").map(_.text).toList,
        (disorder \\ "AverageAgeOfDeathList" \ "AverageAgeOfDeath" \ "Name").map(_.text).toList,
        (disorder \\ "TypeOfInheritanceList" \ "TypeOfInheritance" \ "Name").map(_.text).toList
      )
    }
  }

}

case class OrphanetGeneAssociation(disorder_id: Long,
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
                                   gene_locus_key: Long)

case class OrphanetDiseaseHistory(disorder_id: Long,
                                  orpha_code: Long,
                                  expert_link: String,
                                  name: String,
                                  disorder_type_id: Long,
                                  disorder_type_name: String,
                                  disorder_group_id: Long,
                                  disorder_group_name: String,
                                  average_age_of_onset: List[String],
                                  average_age_of_death: List[String],
                                  type_of_inheritance: List[String])
