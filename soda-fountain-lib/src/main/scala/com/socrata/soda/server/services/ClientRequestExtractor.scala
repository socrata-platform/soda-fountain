package com.socrata.soda.server.services


import com.socrata.http.server.{HttpResponse}
import com.socrata.soql.types.SoQLType
import java.io.{IOException, UnsupportedEncodingException, Reader}
import com.rojoma.json.util.{JsonArrayIterator, JsonUtil}
import com.rojoma.json.ast._
import com.socrata.soql.environment.{ColumnName, TypeName}
import scala.collection.Map
import scala.collection.immutable.VectorBuilder
import scala.Some
import com.rojoma.json.ast.JString
import com.rojoma.json.io._
import javax.servlet.http.HttpServletRequest
import javax.activation.{MimeTypeParseException, MimeType}
import scala.Some
import com.rojoma.json.ast.JString
import scala.Some
import com.rojoma.json.ast.JString
import com.rojoma.json.io.TokenIdentifier
import com.rojoma.json.io.TokenString

object ClientRequestExtractor {

  case class DatasetSpec( resourceName:String,
                          name:String,
                          description:Option[String],
                          rowId:Option[String],
                          locale:String,
                          columns:Seq[ColumnSpec])
  case class ColumnSpec(  fieldName:ColumnName,
                          name:String,
                          description:Option[String],
                          dataType:SoQLType)

  object DatasetSpec {
    def apply(request: HttpServletRequest, approxLimit: Long) : Either[Seq[String], DatasetSpec] = {
      try {
        jsonSingleObjectStream(request, approxLimit) match {
          case Right(obj) =>
            val dex = new DatasetExtractor(obj.fields)
            val fields = (
              dex.resourceName,
              dex.name,
              dex.description,
              dex.rowId,
              dex.locale,
              dex.columns
              )
            fields match {
              case (Right(rn), Right(n), Right(d), Right(r), Right(loc), Right(c)) => {
                Right(DatasetSpec(rn, n, d, r, loc, c))
              }
              case _ => {
                val ers = fields.productIterator.collect {
                  case Left(msg : String) => Seq(msg)
                  case Left(msgs: Seq[String]) => msgs
                  case Left(a: Any) => Seq("error with " + a.toString)
                }
                Left(ers.toArray.flatten.toSeq)
              }
            }
          case Left(err) => Left(Seq(err))
        }
      } catch {
        case e: IOException => Left(Seq("could not read column specification: " + e.getMessage))
        case e: JsonReaderException => Left(Seq("could not read column specification as JSON: " + e.getMessage))
        case e: ReaderExceededBound => Left(Seq(e.MSG))
      }
    }
  }

  object ColumnSpec {

    implicit class EitherPartition[A](underlying: Iterator[A]) {
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

    def apply(jval: JValue) : Either[Seq[String], ColumnSpec] = {
      jval match {
        case JObject(map) => {
          val cex = new ColumnExtractor(map)
          val fields = (
            cex.fieldNamme,
            cex.name,
            cex.description,
            cex.datatype
          )
          fields match {
            case (Right(fieldName), Right(name), Right(description), Right(datatype)) =>
              Right(new ColumnSpec(ColumnName(fieldName), name, description, SoQLType.typesByName(TypeName(datatype))))
            case _ =>  Left(fields.productIterator.collect { case Left(msg:String) => msg}.toSeq)
          }
        }
        case _ => Left(Seq("column specification could not be read as JSON object"))
      }
    }
    def array(request: HttpServletRequest, approxLimit: Long) : Either[Seq[String], Seq[ColumnSpec]] = {
      try {
        jsonArrayValuesStream(request, approxLimit) match {
          case Right(boundedIt) => array(boundedIt)
          case Left(err) => Left(Seq(err))
        }
      }
      catch {
        case e: IOException => Left(Seq("could not read column specification: " + e.getMessage))
        case e: JsonReaderException => Left(Seq("could not read column specification as JSON: " + e.getMessage))
        case e: ReaderExceededBound => Left(Seq(e.MSG))
      }
    }
    def array( jvals: Iterator[JValue]) : Either[Seq[String], Seq[ColumnSpec]] = {
      val mapped = jvals.map(ColumnSpec(_))
      val (badColumns, goodColumns) = mapped.divide(identity)
      if (badColumns.flatten.nonEmpty ) {
        Left(badColumns.flatten )
      }
      else {
        Right(goodColumns)
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
        case JArray(Seq(JString(idString))) => Right(Some(idString))
        case _ => Left("An optional row_identifer was included but could not be read - it must be an array containing a single string (the name of the row_identifier column).")
      }
      case None => Right(None)
    }
    def locale = map.get("locale") match {
      case Some(jval) => jval match {
        case JString(loc) => Right(loc)
        case _ => Left("An optional locale description was included but could not be read - it must be a string.")
      }
      case None => Right("en_US")
    }

    def columns: Either[Seq[String], Seq[ColumnSpec]] = {
      map.get("columns") match {
        case Some(JArray(jvals)) => {
          ColumnSpec.array(jvals.iterator)
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

  private def streamJson(req: HttpServletRequest, approximateMaxDatumBound: Long): Either[String, BoundedReader] = {
    val nullableContentType = req.getContentType
    if(nullableContentType == null)
      return Left("req.content-type.missing")
    val contentType =
      try { new MimeType(nullableContentType) }
      catch { case _: MimeTypeParseException =>
        return Left("req.content-type.unparsable")
      }
    if(!contentType.`match`("application/json")) {
      return Left("req.content-type.not-json")
    }
    val reader =
      try { req.getReader }
      catch { case _: UnsupportedEncodingException =>
        return Left("req.content-type.unknown-charset")
      }
    Right(new BoundedReader(reader, approximateMaxDatumBound))
  }

  def jsonSingleObjectStream(req: HttpServletRequest, approximateMaxDatumBound: Long): Either[String, JObject] = {
    streamJson(req, approximateMaxDatumBound) match {
      case Right(boundedReader) =>
        JsonReader.fromEvents(new JsonEventIterator(new BlockJsonTokenIterator(boundedReader))) match {
          case obj: JObject => Right(obj)
          case _ => Left("req.content.json.not-single-object")
        }
      case Left(e) => Left(e)
    }
  }

  def jsonArrayValuesStream(req: HttpServletRequest, approximateMaxDatumBound: Long): Either[String, Iterator[JValue]] = {
    streamJson(req, approximateMaxDatumBound) match {
      case Right(boundedReader) =>
        val it = JsonArrayIterator[JValue](new JsonEventIterator(new BlockJsonTokenIterator(boundedReader)))
        val boundedIt = it.map { ev => boundedReader.resetCount(); ev }
        Right(boundedIt)
      case Left(e) => Left(e)
    }
  }

  class ReaderExceededBound(val bytesRead: Long) extends Exception { val MSG = "input length exceeded allowable limit" }
  class BoundedReader(underlying: Reader, bound: Long) extends Reader {
    private var count = 0L
    private def inc(n: Int) {
      count += n
      if(count > bound) throw new ReaderExceededBound(count)
    }

    override def read() =
      underlying.read() match {
        case -1 => -1
        case n => inc(1); n
      }

    def read(cbuf: Array[Char], off: Int, len: Int): Int =
      underlying.read(cbuf, off, len) match {
        case -1 => -1
        case n => inc(n); n
      }

    def close() {
      underlying.close()
    }

    def resetCount() {
      count = 0
    }
  }

}