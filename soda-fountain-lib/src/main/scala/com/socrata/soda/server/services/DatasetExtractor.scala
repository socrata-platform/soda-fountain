package com.socrata.soda.server.services

import com.socrata.soql.types.SoQLType
import java.io.Reader
import com.rojoma.json.util.JsonUtil
import com.rojoma.json.ast._
import com.socrata.soql.environment.TypeName
import scala.collection.Map
import scala.collection.immutable.VectorBuilder
import scala.Some
import com.rojoma.json.ast.JString

case class DatasetSpec( resourceName:String,
                        name:String,
                        description:Option[String],
                        rowId:Option[Either[BigDecimal, String]],
                        locale:Option[String],
                        columns:Seq[ColumnSpec])
case class ColumnSpec(  fieldName:String,
                        name:String,
                        description:Option[String],
                        dataType:SoQLType)

object DatasetSpec {
  def apply(reader: Reader) : Either[Seq[String], DatasetSpec] = {
    try {
      val fields = JsonUtil.readJson[Map[String,JValue]](reader)
      fields match {
        case Some(map) => {
          val dex = new DatasetExtractor(map)
          val vals = (
            dex.resourceName,
            dex.name,
            dex.description,
            dex.rowId,
            dex.locale,
            dex.columns
            )
          vals match {
            case (Right(rn), Right(n), Right(d), Right(r), Right(loc), Right(c)) => {
              Right(DatasetSpec(rn, n, d, r, loc, c))
            }
            case _ => {
              val ers = vals.productIterator.collect {
                case Left(msg : String) => Seq(msg)
                case Left(msgs: Seq[String]) => msgs
                case Left(a: Any) => Seq("error with " + a.toString)
              }
              Left(ers.toArray.flatten.toSeq)
            }
          }
        }
        case None => Left(Seq("Could not read dataset specification as JSON object"))
      }
    } catch {
      case e: Exception => Left(Seq("could not parse as JSON: " + e.getMessage))
      case _: Throwable => Left(Seq("could not parse as JSON"))
    }
  }
}

object ColumnSpec {
  def apply(jval: JValue) : Either[Seq[String], ColumnSpec] = {
    jval match {
      case JObject(map) => {
        val cex = new ColumnExtractor(map)
        val vals = (
          cex.fieldNamme,
          cex.name,
          cex.description,
          cex.datatype
        )
        vals match {
          case (Right(fieldName), Right(name), Right(description), Right(datatype)) =>
            Right(new ColumnSpec(fieldName, name, description, SoQLType.typesByName(TypeName(datatype))))
          case _ =>  Left(vals.productIterator.collect { case Left(msg:String) => msg}.toSeq)
        }
      }
      case _ => Left(Seq("column specification could not be read as JSON object"))
    }
  }
}

class DatasetExtractor(map: Map[String, JValue]){
  def resourceName : Either[String, String] = map.get("resource_name") match {
    case Some(JString(rn)) => Right(rn)
    case _ => Left("Dataset resouce name not found: 'resource_name' is a required key, and its value must be a string")
  }
  def name = map.get("name") match {
    case Some(JString(n)) => Right(n)
    case _ => Left("Dataset name not found: 'name' is a required key, and its value must be a string")
  }
  def description = map.get("description") match {
    case Some(jval) => jval match {
      case JString(desc) => Right(Some(desc))
      case _ => Left("An optional dataset description was included but could not be read - it must be a string.")
    }
    case None => Right(None)
  }
  def rowId = map.get("row_identifier") match {
    case Some(jval) => jval match {
      case JString(idString) => Right(Some(Right(idString)))
      case JNumber(idNum) => Right(Some(Left(idNum)))
      case _ => Left("An optional row_identifer was included but could not be read - it must be a string.")
    }
    case None => Right(None)
  }
  def locale = map.get("locale") match {
    case Some(jval) => jval match {
      case JString(loc) => Right(Some(loc))
      case _ => Left("An optional locale description was included but could not be read - it must be a string.")
    }
    case None => Right(None)
  }

  implicit class EitherPartition[A](underlying: Seq[A]) {
    def divide[B, C](f: A => Either[B, C]): (Seq[B], Seq[C]) = {
      val lefts = new VectorBuilder[B]
      val rights = new VectorBuilder[C]
      underlying.foreach { x =>
        f(x) match {
          case Left(l) => lefts += l
          case Right(r) => rights += r
        }
      }
      (lefts.result(), rights.result())
    }
  }

  def columns: Either[Seq[String], Seq[ColumnSpec]] = {
    map.get("columns") match {
      case Some(JArray(jvals)) => {
        val mapped = jvals.map(ColumnSpec(_))
        val (badColumns, goodColumns) = mapped.divide(identity)
        if (badColumns.flatten.nonEmpty ) {
          Left(badColumns.flatten )
        }
        else {
          Right(goodColumns)
        }
      }
      case _ => Left(Seq("Dataset columns specification not found: 'columns' is a required key, and its value must be a json array"))
    }
  }
}

class ColumnExtractor(jobj: Map[String, JValue]){
  def name = jobj.get("name") match {
    case Some(JString(n)) => Right(n)
    case _ => Left("Column name not found: 'name' is a required key, and its value must be a string")
  }
  def fieldNamme = jobj.get("field_name") match {
    case Some(JString(fn)) => Right(fn)
    case _ => Left("Column field name not found: 'field_name' is a required key, and its value must be a string")
  }
  def datatype = jobj.get("datatype") match {
    case Some(JString(dt)) => Right(dt)
    case _ => Left("Column datatype not found: 'datatype' is a required key, and its value must be a string")
  }
  def description = jobj.get("description") match {
    case Some(jval) => jval match {
      case JString(desc) => Right(Some(desc))
      case _ => Left("An optional column description was included but could not be read - it must be a string.")
    }
    case None => Right(None)
  }
}
