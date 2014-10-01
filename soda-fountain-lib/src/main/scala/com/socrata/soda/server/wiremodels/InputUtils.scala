package com.socrata.soda.server.wiremodels

import scala.{collection => sc}
import java.io.{IOException, UnsupportedEncodingException, Reader}
import com.rojoma.json.io.{JsonReaderException, JsonReader, FusedBlockJsonEventIterator}
import com.socrata.soda.server.InputNormalizer
import javax.servlet.http.HttpServletRequest
import com.socrata.soda.server.errors._
import com.rojoma.json.ast.{JValue, JObject}
import com.rojoma.json.util.JsonArrayIterator
import javax.activation.{MimeTypeParseException, MimeType}
import com.socrata.http.common.util.{TooMuchDataWithoutAcknowledgement, AcknowledgeableReader}
import com.socrata.soda.server.errors.UnparsableContentType
import com.socrata.soda.server.errors.ContentTypeNotJson
import com.socrata.http.server.implicits._

object InputUtils {
  def eventIterator(reader: Reader) = new FusedBlockJsonEventIterator(reader).map(InputNormalizer.normalizeEvent)

  private def streamJson(req: HttpServletRequest, approximateMaxDatumBound: Long): Either[SodaError, AcknowledgeableReader] = {
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
    val reader =
      req.updateCharacterEncoding() match {
        case None =>
          req.getReader()
        case Some(err) =>
          return Left(ContentTypeUnsupportedCharset(contentType))
      }
    Right(new AcknowledgeableReader(reader, approximateMaxDatumBound))
  }

  def jsonSingleObjectStream(req: HttpServletRequest, approximateMaxDatumBound: Long): Either[SodaError, JObject] = {
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
  def jsonArrayValuesStream(req: HttpServletRequest, approximateMaxDatumBound: Long): Either[SodaError, Iterator[JValue]] = {
    streamJson(req, approximateMaxDatumBound) match {
      case Right(boundedReader) =>
        val it = JsonArrayIterator[JValue](eventIterator(boundedReader))
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
