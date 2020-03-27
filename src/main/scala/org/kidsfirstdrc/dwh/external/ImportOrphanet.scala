package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.SparkSession

import scala.xml.{Elem, XML}

object ImportOrphanet extends App {

  val input = args(1)
  val output = args(2)

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import Orphanet").getOrCreate()
  run(input, output)

  def run(input: String, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val doc = XML.loadFile(input)

    val outputs = parseXML(doc)
    outputs.toDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("parquet")
      .option("path", s"$output/orphanet_gene_set")
      .saveAsTable("variant.orphanet_gene_set")

  }


  def parseXML(doc: Elem):Seq[OrphanetOutput] = {
    for {
      disorder <- doc \\ "DisorderList" \\ "Disorder"
      orphaNumber <- disorder \ "OrphaNumber"
      name <- disorder \ "Name"
      disorder_type <- disorder \ "DisorderType" \ "Name"
      disorder_group <- disorder \ "DisorderGroup" \ "Name"
      geneAssociation <- disorder \ "DisorderGeneAssociationList" \ "DisorderGeneAssociation"
      genes <- geneAssociation \ "Gene"
      ensemblId = (genes \ "ExternalReferenceList" \ "ExternalReference").find(node => (node \ "Source").text == "Ensembl").map(_ \ "Reference").map(_.text)

    } yield {
      OrphanetOutput(
        disorder.attribute("id").get.text.toInt,
        orphaNumber.text.toInt,
        (disorder \ "ExpertLink").headOption.map(_.text),
        name.text,
        disorder_type.text,
        disorder_group.text,
        (genes \ "Symbol").text,
        ensemblId,
        (geneAssociation \ "DisorderGeneAssociationType" \ "Name").headOption.map(_.text),
        (geneAssociation \ "DisorderGeneAssociationType").headOption.flatMap(_.attribute("id")).map(_.text.toInt),
        (geneAssociation \ "DisorderGeneAssociationStatus" \ "Name").headOption.map(_.text)
      )

    }
  }
}

