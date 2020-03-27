package org.kidsfirstdrc.dwh.external

import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.xml.XML


class ImportOrphanetSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  "run" should "update orhanet table" in {

    withOutputFolder("output") { output =>
      val input = getClass.getResource("/sample_orphanet.xml").getFile

      ImportOrphanet.run(input, output)
      val orphanets = spark.table("variant.orphanet_gene_set")

      import spark.implicits._
      orphanets.as[OrphanetOutput].collect() should contain theSameElementsAs Seq(
        OrphanetOutput(17601, 166024, Some("http://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=en&Expert=166024"), "Multiple epiphyseal dysplasia, Al-Gazali type", "Disease", "Disorder", "KIF7", Some("ENSG00000166813"), Some("Disease-causing germline mutation(s) in"), Some(17949), Some("Assessed")),
        OrphanetOutput(10, 206, Some("http://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=en&Expert=206"), "Crohn disease", "Disease", "Disorder", "IL6", Some("ENSG00000136244"), Some("Modifying germline mutation in"), Some(17967), Some("Assessed")),
        OrphanetOutput(10, 206,Some("http://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=en&Expert=206"), "Crohn disease", "Disease", "Disorder", "IL23R", Some("ENSG00000162594"), Some("Major susceptibility factor in"), Some(17961), Some("Assessed"))
      )

    }
  }

  "parseXML" should "return an OrphanetOutput" in {
   val content =  """
      |    <DisorderList count="3841">
      |        <Disorder id="17601">
      |            <OrphaNumber>166024</OrphaNumber>
      |            <ExpertLink lang="en">http://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=en&amp;Expert=166024</ExpertLink>
      |            <Name lang="en">Multiple epiphyseal dysplasia, Al-Gazali type</Name>
      |            <DisorderType id="21394">
      |                <Name lang="en">Disease</Name>
      |            </DisorderType>
      |            <DisorderGroup id="36547">
      |                <Name lang="en">Disorder</Name>
      |            </DisorderGroup>
      |            <DisorderGeneAssociationList count="1">
      |                <DisorderGeneAssociation>
      |                    <Gene id="20160">
      |                        <Name lang="en">kinesin family member 7</Name>
      |                        <Symbol>KIF7</Symbol>
      |                        <GeneType id="25993">
      |                            <Name lang="en">gene with protein product</Name>
      |                        </GeneType>
      |                        <ExternalReferenceList count="1">
      |                            <ExternalReference id="57240">
      |                                <Source>Ensembl</Source>
      |                                <Reference>ENSG00000166813</Reference>
      |                            </ExternalReference>
      |                        </ExternalReferenceList>
      |                    </Gene>
      |                    <DisorderGeneAssociationType id="17949">
      |                        <Name lang="en">Disease-causing germline mutation(s) in</Name>
      |                    </DisorderGeneAssociationType>
      |                    <DisorderGeneAssociationStatus id="17991">
      |                        <Name lang="en">Assessed</Name>
      |                    </DisorderGeneAssociationStatus>
      |                    <DisorderGeneAssociationType id="17949">
      |                        <Name lang="en">Disease-causing germline mutation(s) in</Name>
      |                    </DisorderGeneAssociationType>
      |                    <DisorderGeneAssociationStatus id="17991">
      |                        <Name lang="en">Assessed</Name>
      |                    </DisorderGeneAssociationStatus>|
      |                </DisorderGeneAssociation>
      |            </DisorderGeneAssociationList>
      |        </Disorder>
      |    </DisorderList>
      |""".stripMargin

    val doc = XML.loadString(content)
    ImportOrphanet.parseXML(doc) should contain only OrphanetOutput(17601,166024,Some("http://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=en&Expert=166024"),"Multiple epiphyseal dysplasia, Al-Gazali type","Disease","Disorder","KIF7",Some("ENSG00000166813"),Some("Disease-causing germline mutation(s) in"),Some(17949),Some("Assessed"))
  }

}
