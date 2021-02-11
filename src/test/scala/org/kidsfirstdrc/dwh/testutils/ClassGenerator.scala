package org.kidsfirstdrc.dwh.testutils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.kidsfirstdrc.dwh.testutils.ClassGenerator.getClass

import java.io._
import java.time.LocalDateTime

object ClassGenerator {

  def getType: PartialFunction[DataType, String] = {
    case StringType                           => "String"
    case FloatType                            => "Float"
    case IntegerType                          => "Int"
    case BooleanType                          => "Boolean"
    case DoubleType                           => "Double"
    case LongType                             => "Long"
    case DecimalType()                        => "Double"
    case ArrayType(StringType, _)             => "List[String]"
    case ArrayType(FloatType, _)              => "List[Float]"
    case ArrayType(IntegerType, _)            => "List[Int]"
    case ArrayType(BooleanType, _)            => "List[Boolean]"
    case ArrayType(DoubleType, _)             => "List[Double]"
    case ArrayType(LongType, _)               => "List[Long]"
    case MapType(StringType,LongType, _)      => "Map[String,Long]"
    case MapType(StringType,DecimalType(), _) => "Map[String,BigDecimal]"
    case MapType(StringType,ArrayType(StringType,_),_) =>"Map[String, List[String]]"
  }

  def getValue: PartialFunction[(String, Row, DataType), String] = {
    case (name, values, StringType)                                    => "\"" + values.getAs(name) + "\""
    case (name, values, FloatType)                                     => values.getAs(name)
    case (name, values, IntegerType)                                   => values.getAs(name)
    case (name, values, BooleanType)                                   => values.getAs(name)
    case (name, values, DoubleType)                                    => values.getAs(name)
    case (name, values, LongType)                                      => values.getAs(name)
    case (name, values, DecimalType())                                 => values.getAs(name)
    case (name, values, ArrayType(StringType,_))                       => s"""List(${values.getAs[List[String]](name).mkString("\"", "\", \"", "\"")})"""
    case (name, values, ArrayType(FloatType,_))                        => values.getAs[List[Float]](name).mkString("List(", ", ", ")")
    case (name, values, ArrayType(IntegerType,_))                      => values.getAs[List[Int]](name).mkString("List(", ", ", ")")
    case (name, values, ArrayType(BooleanType,_))                      => values.getAs[List[Boolean]](name).mkString("List(", ", ", ")")
    case (name, values, ArrayType(DoubleType,_))                       => values.getAs[List[Boolean]](name).mkString("List(", ", ", ")")
    case (name, values, ArrayType(LongType,_))                         => values.getAs[List[Long]](name).mkString("List(", ", ", ")")
    case (name, values, MapType(StringType,LongType, _))               => values.getAs[Map[String, Long]](name).mkString("Map(", ", ", ")")
    case (name, values, MapType(StringType,DecimalType(), _))          => values.getAs[Map[String, BigDecimal]](name).mkString("Map(", ", ", ")")
    case (name, values, MapType(StringType,ArrayType(StringType,_),_)) => values.getAs[Map[String, List[String]]](name).mkString("Map(", ", ", ")")
  }

  def oneClassString(className: String, df: DataFrame): String = {
    val values: Row = df.collect().headOption.getOrElse(throw new IllegalArgumentException("input dataframe empty."))
    val fields: Array[String] = {
      df.schema.fields.map {
        case StructField(name, dataType, _, _) if getValue.isDefinedAt(name, values, dataType) && getType.isDefinedAt(dataType)=>
          if(values.getAs(name) == null)
            s"""`$name`: Option[${getType(dataType)}] = None"""
          else
            s"""`$name`: ${getType(dataType)} = ${getValue(name, values, dataType)}"""

        case StructField(name, StructType(_), _, _) => s"""`$name`: ${name.toUpperCase} = ${name.toUpperCase}() """
        case StructField(name, ArrayType(StructType(_), _), _, _) => s"""`$name`: List[${name.toUpperCase}] = List(${name.toUpperCase}()) """
        case structField: StructField => structField.toString()

      }
    }

    val spacing = s"case class $className(".toCharArray.map(_ => " ")

    s"""
       |case class $className(${fields.mkString("", s",\n${spacing.mkString}" , ")")}
       |""".stripMargin
  }

  def getCaseClassFileContent(packageName: String, className: String, df: DataFrame): String = {

    val mainClass = oneClassString(className, df)

    val nestedClasses = df.schema.fields.filter {
      case StructField(_, StructType(_), _, _) => true
      case StructField(_, ArrayType(StructType(_), _), _, _) => true
      case _ => false
    }.map {
      case StructField(name, StructType(_), _, _) => oneClassString(name.toUpperCase, df.select(s"${name}.*"))
      case StructField(name, ArrayType(StructType(_), _), _, _) =>
        oneClassString(name.toUpperCase, df.withColumn(name, explode(col(name))).select(s"${name}.*"))
      case s => s.toString()
    }

    s"""/**
       | * Generated by [[${this.getClass.getCanonicalName.replace("$", "")}]]
       | * on ${LocalDateTime.now()}
       | */
       |package $packageName
       |
       |$mainClass
       |${nestedClasses.mkString("\n")}
       |""".stripMargin
  }

  def writeCLassFile(packageName: String,
                     className: String,
                     df: DataFrame,
                     rootFolder: String = getClass.getResource(".").getFile): Unit = {

    val classContent = getCaseClassFileContent(packageName, className, df)
    val path: String = (rootFolder + packageName.replace(".", "/")+ s"/$className.scala")

    println(
      s"""writting file: $path :
         |$classContent
         |""".stripMargin)
    val file = new File(path)
    file.createNewFile()
    val pw = new PrintWriter(file)
    pw.write(classContent)
    pw.close()

  }


  def printCaseClassFromDataFrame(packageName: String, className: String, df: DataFrame): Unit = {
    println(getCaseClassFileContent(packageName, className, df))
  }
}

object ClassGeneratorImplicits {
  implicit class ClassGenDataFrameOps(df: DataFrame) {
    def writeCLassFile(packageName: String,
                       className: String,
                       rootFolder: String): Unit =
      ClassGenerator.writeCLassFile(packageName, className, df, rootFolder)
  }
}

