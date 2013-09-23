package com.socrata.soda.server.errors

import javax.servlet.http.HttpServletResponse._
import com.rojoma.json.ast._
import com.rojoma.json.codec.JsonCodec
import javax.activation.MimeType
import com.socrata.soql.environment.TypeName
import com.socrata.http.server.util.EntityTag

case class ResourceNotModified(override val etags: Seq[EntityTag], override val vary: Option[String])
  extends SodaError(SC_NOT_MODIFIED, "not-modified")

case object EtagPreconditionFailed
  extends SodaError(SC_PRECONDITION_FAILED, "precondition-failed")

case class GeneralNotFoundError(path: String)
  extends SodaError(SC_NOT_FOUND, "not-found", "path" -> JString(path))

case class InternalError(tag: String)
  extends SodaError(SC_INTERNAL_SERVER_ERROR, "internal-error",
    "tag" -> JString(tag))

case class HttpMethodNotAllowed(method: String, allowed: TraversableOnce[String])
  extends SodaError(SC_METHOD_NOT_ALLOWED, "method-not-allowed",
    "method" -> JString(method),
    "allowed" -> JsonCodec.toJValue(allowed.toSeq))

case object NoContentType extends SodaError("req.content-type.missing")

case class UnparsableContentType(contentType: String)
  extends SodaError("req.content-type.unparsable",
    "content-type" -> JString(contentType))

case class ContentTypeNotJson(contentType: MimeType)
  extends SodaError(SC_UNSUPPORTED_MEDIA_TYPE, "req.content-type.not-json",
    "content-type" -> JString(contentType.toString))

case class ContentTypeUnsupportedCharset(contentType: MimeType)
  extends SodaError(SC_UNSUPPORTED_MEDIA_TYPE, "req.content-type.unknown-charset",
    "content-type" -> JString(contentType.toString))

case class MalformedJsonBody(row: Int, column: Int)
  extends SodaError("req.body.malformed-json",
    "row" -> JNumber(row),
    "column" -> JNumber(column))

case class BodyTooLarge(limit: Long)
  extends SodaError(SC_REQUEST_ENTITY_TOO_LARGE, "req.body.too-large",
    "limit" -> JNumber(limit))

case class ContentNotSingleObject(value: JValue)
  extends SodaError("req.content.json.not-object",
    "value" -> value)

case class DatasetSpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaError("soda.dataset.maltyped",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class ColumnSpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaError("soda.column.maltyped",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class ColumnSpecUnknownType(typeName: TypeName)
  extends SodaError("soda.column.unknown-type",
    "type" -> JString(typeName.name))
