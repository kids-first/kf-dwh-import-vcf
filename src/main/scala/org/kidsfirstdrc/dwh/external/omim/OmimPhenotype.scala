package org.kidsfirstdrc.dwh.external.omim

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

case class OmimPhenotype(name: String, omim_id: String, inheritence: Option[Seq[String]])

object OmimPhenotype {

  val pheno_regexp = "(.*),\\s(\\d*)\\s\\([1234]\\)(?:,\\s(.*))?".r

  def mapInheritence(inheritance: String): Option[Seq[String]] = {
    if (inheritance == null) None
    else {
      val s = inheritance.split(", ") flatMap {
        case "Y-linked" => Some("YL")
        case "X-linked" => Some("XL")
        case "Y-linked recessive" => Some("YLR")
        case "Y-linked dominant" => Some("YLD")
        case "X-linked dominant" => Some("XLD")
        case "X-linked recessive" => Some("XLR")
        case "Pseudoautosomal recessive" => Some("PR")
        case "Pseudoautosomal dominant" => Some("PD")
        case "Autosomal recessive" => Some("AR")
        case "Autosomal dominant" => Some("AD")
        case "Mitochondrial" => Some("Mi")
        case "Multifactorial" => Some("Mu")
        case "Inherited chromosomal imbalance" => Some("ICB")
        case "Somatic mutation" => Some("Smu")
        case "Isolated cases" => Some("IC")
        case "Somatic mosaicism" => Some("SMo")
        case "Digenic recessive" => Some("DR")
        case "Digenic dominant" => Some("DD")
        case "?Autosomal dominant" => Some("?AD")
        case "?X-linked recessive" => Some("?AD")
      }
      if (s.nonEmpty) Some(s.distinct) else None
    }
  }

  val parse_pheno = udf { raw: String =>
    raw match {
      case pheno_regexp(name, omim_id, inheritance) => Some(OmimPhenotype(name, omim_id, mapInheritence(inheritance)))
      case _ => None
    }
  }

}