package com.socrata.soda.server.errors

import javax.servlet.http.HttpServletResponse._
import com.rojoma.json.ast._
import com.rojoma.json.codec.JsonCodec
import javax.activation.MimeType
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.http.server.util.EntityTag
import com.socrata.soda.server.id.{RollupName, ResourceName, RowSpecifier}
import com.socrata.soda.server.wiremodels.ComputationStrategyType

case class ResourceNotModified(override val etags: Seq[EntityTag], override val vary: Option[String], override val hasContent: Boolean = false)
  extends SodaError(SC_NOT_MODIFIED, "not-modified")

case object EtagPreconditionFailed
  extends SodaError(SC_PRECONDITION_FAILED, "precondition-failed")

case object SchemaInvalidForMimeType
  extends SodaError(SC_NOT_ACCEPTABLE, "schema-invalid-for-mime-type")

case class GeneralNotFoundError(path: String)
  extends SodaError(SC_NOT_FOUND, "not-found", "path" -> JString(path))

case class InternalError(th: Throwable, tag: String)
  extends SodaError(SC_INTERNAL_SERVER_ERROR, "internal-error",
    "tag" -> JString(tag),
    "errorMessage" -> JString(InternalError.getException(th).getMessage),
    "errorClass"   -> JString(InternalError.getException(th).getClass.getCanonicalName),
    "stackTrace"   -> JArray(InternalError.getException(th).getStackTrace
                                                           .map(x => JString(x.toString)).toSeq))

object InternalError {
  def getException(th: Throwable): Throwable =
    Option(th).getOrElse(new RuntimeException("Missing Exception") {
        override val getStackTrace = Array.empty[java.lang.StackTraceElement]
      })

  def apply(tag: String): InternalError = apply(null, tag)
}

case class HttpMethodNotAllowed(method: String, allowed: TraversableOnce[String])
  extends SodaError(SC_METHOD_NOT_ALLOWED, "method-not-allowed",
    "method" -> JString(method),
    "allowed" -> JsonCodec.toJValue(allowed.toSeq))

case object NoContentType extends SodaError("req.content-type.missing")

case class BadParameter(param: String, value: String)
  extends SodaError(SC_BAD_REQUEST, "req.bad-parameter",
    "parameter" -> JString(param),
    "value" -> JString(value))

case class ErrorReportedByQueryCoordinator(code: Int, value: QueryCoordinatorError)
  extends SodaError(code, value.errorCode, value.data.toMap)

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

case class ComputationStrategySpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaError("soda.column_computation_strategy.maltyped",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class ComputationStrategySpecUnknownType(typ: String)
  extends SodaError("soda.computation-strategy.unknown-type",
    "type" -> JString(typ))

case class RollupSpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaError("soda.rollup.maltyped",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class RollupCreationFailed(error: String)
  extends SodaError("soda.rollup.creation-failed",
    "error" -> JString(error))

case class RollupColumnNotFound(value: ColumnName)
  extends SodaError("soda.rollup.column-not-found",
    "value" -> JString(value.name))

case class RollupNotFound(value: RollupName)
  extends SodaError("soda.rollup.not-found",
    "value" -> JString(value.name))

case class NonUniqueRowId(column: String)
  extends SodaError("soda.column.not-unique",
    "column" -> JString(column)
  )

case class ColumnSpecUnknownType(typeName: TypeName)
  extends SodaError("soda.column.unknown-type",
    "type" -> JString(typeName.name))

case class DatasetNotFound(value: ResourceName)
  extends SodaError(SC_NOT_FOUND, "soda.dataset.not-found",
    "dataset" -> JString(value.name))

case class ColumnNotFound(resourceName: ResourceName, columnName: ColumnName)
  extends SodaError(SC_NOT_FOUND, "soda.dataset.column-not-found",
    "dataset" -> JString(resourceName.name),
    "column"  -> JString(columnName.name))

case class RowNotFound(value: RowSpecifier)
  extends SodaError(SC_NOT_FOUND, "soda.row.not-found",
     "value" -> JString(value.underlying))

/**
 * Column not found in a row operation
 */
case class RowColumnNotFound(value: ColumnName)
  extends SodaError(SC_BAD_REQUEST, "soda.row.column-not-found",
    "value" -> JString(value.name))

case class ComputationHandlerNotFound(typ: ComputationStrategyType.Value)
  extends SodaError(SC_BAD_REQUEST, "soda.column.computation-handler-not-found",
    "computation-strategy" -> JString(typ.toString))

case class ComputedColumnNotWritable(value: ColumnName)
  extends SodaError(SC_BAD_REQUEST, "soda.row.computed-column-not-writable",
    "value" -> JString(value.name))

case class NotAComputedColumn(value: ColumnName)
  extends SodaError(SC_BAD_REQUEST, "soda.column.not-a-computed-column",
    "value" -> JString(value.name))

case object DeleteWithoutPrimaryKey
  extends SodaError(SC_BAD_REQUEST, "soda.row.delete-without-primary-key")

case class UpsertRowNotAnObject(obj: JValue)
  extends SodaError(SC_BAD_REQUEST, "soda.row.upsert-row-not-an-object",
    "value" -> obj)