package com.socrata.soda.server.wiremodels

import java.io.{IOException, Reader}

import scala.{collection => sc}

import com.rojoma.json.v3.ast.{JObject, JValue}
import com.rojoma.json.v3.io._
import com.rojoma.json.v3.util.JsonArrayIterator
import com.socrata.http.common.util.{AcknowledgeableReader, TooMuchDataWithoutAcknowledgement}
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.implicits._
import com.socrata.soda.server._
import com.socrata.soda.server.errors._
import javax.activation.{MimeType, MimeTypeParseException}

object InputUtils {
  def eventIterator(reader: Reader) = new FusedBlockJsonEventIterator(reader).map(InputNormalizer.normalizeEvent)

  private def streamJson(req: HttpRequest, approximateMaxDatumBound: Long): Either[SodaError, AcknowledgeableReader] = {
    val nullableContentType = req.getContentType
    if(nullableContentType == null)
      return Left(NoContentType)
    val contentType =
      try { new MimeType(nullableContentType) }
      catch { case _: MimeTypeParseException =>
        return Left(UnparsableContentType(nullableContentType))
      }
    if(!contentType.`match`("application/json")) {
      return Left(ContentTypeNotJson(contentType))
    }
    req.reader match {
      case Right(reader) =>
        Right(new AcknowledgeableReader(reader, approximateMaxDatumBound))
      case Left(_) =>
        Left(ContentTypeUnsupportedCharset(contentType))
    }
  }

  def jsonSingleObjectStream(req: HttpRequest, approximateMaxDatumBound: Long): Either[SodaError, JObject] = {
    streamJson(req, approximateMaxDatumBound).right.flatMap { boundedReader =>
      try {
        JsonReader.fromEvents(eventIterator(boundedReader)) match {
          case obj: JObject => Right(obj)
          case other => Left(ContentNotSingleObject(other))
        }
      } catch {
        case e: TooMuchDataWithoutAcknowledgement =>
          Left(BodyTooLarge(e.limit))
      }
    }
  }

  /**
   * If the json input is not an array but an object, it will still accept the input and turn the object
   * into an iterator with a single jvalue.  This allows processing of multiple rows and single row.
   * @note The iterator can throw a `TooMuchDataWithoutAcknowledgement` exception if the user
   *       sends an element which cannot be read within `approximateMaxDatumBound` bytes.
   */
  def jsonArrayValuesStream(req: HttpRequest, approximateMaxDatumBound: Long): Either[SodaError, Iterator[JValue]] = {
    streamJson(req, approximateMaxDatumBound) match {
      case Right(boundedReader) =>
        def boundIt[T](it: Iterator[T]) = it.map { ev => boundedReader.acknowledge(); ev }
        val fbJsonEventIt: FusedBlockJsonEventIterator = new FusedBlockJsonEventIterator(boundedReader)
        val jsonEventIt = fbJsonEventIt.map(InputNormalizer.normalizeEvent)
        fbJsonEventIt.head match {
          case StartOfArrayEvent() =>
            Right(boundIt(JsonArrayIterator[JValue](jsonEventIt)))
          case StartOfObjectEvent() =>
            Right(boundIt(Iterator.single(JsonReader.fromEvents(jsonEventIt))))
          case _ =>
            Left(InvalidJsonContent("array, object"))
        }
      case Left(e) => Left(e)
    }
  }

  def catchingInputProblems[T](f: => ExtractResult[T]): ExtractResult[T] =
    try {
      f
    } catch {
      case e: IOException => IOProblem(e)
      case e: JsonReaderException =>
        RequestProblem(MalformedJsonBody(e.row, e.column))
      case e: TooMuchDataWithoutAcknowledgement =>
        RequestProblem(BodyTooLarge(e.limit))
    }

  class ExtractContext(maltyped: (String, String, JValue) => SodaError) {
    def extract[T](v: sc.Map[String, JValue], field: String)(implicit decoder: Decoder[T]): ExtractResult[Option[T]] = {
      v.get(field) match {
        case Some(json) =>
          decoder(json) match {
            case Right(t) => Extracted(Some(t))
            case Left((expected, got)) => RequestProblem(maltyped(field, expected, got))
          }
        case None =>
          Extracted(None)
      }
    }
  }
}
