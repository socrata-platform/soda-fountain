package com.socrata.soda.server.wiremodels

import java.io.{IOException, Reader}

import scala.{collection => sc}

import com.rojoma.json.v3.ast.{JObject, JValue}
import com.rojoma.json.v3.io.{StartOfArrayEvent, FusedBlockJsonEventIterator, JsonReader, JsonReaderException}
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
   * @note The iterator can throw a `TooMuchDataWithoutAcknowledgement` exception if the user
   *       sends an element which cannot be read within `approximateMaxDatumBound` bytes.
   */
  def jsonValueOrArrayValuesStream(req: HttpRequest, approximateMaxDatumBound: Long): Either[SodaError, Iterator[JValue]] = {
    streamJson(req, approximateMaxDatumBound) match {
      case Right(boundedReader) =>
        val evIt = eventIterator(boundedReader).buffered
        val it =
          if(evIt.hasNext && evIt.head.isInstanceOf[StartOfArrayEvent]) {
            JsonArrayIterator[JValue](evIt)
          } else {
            // "Iterator.empty ++" keeps the laziness properties of this branch the same;
            // the object will not be read until it is demanded, if it is a non-atom.
            Iterator.empty ++ Iterator.single(JsonReader.fromEvents(evIt))
          }
        val boundedIt = it.map { ev => boundedReader.acknowledge(); ev }
        Right(boundedIt)
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
