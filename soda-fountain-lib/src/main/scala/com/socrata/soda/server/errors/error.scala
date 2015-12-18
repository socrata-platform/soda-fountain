package com.socrata.soda.server.errors

import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse._

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec._
import com.socrata.http.server.util.EntityTag
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorError
import com.socrata.soda.server.id.{ResourceName, RollupName, RowSpecifier}
import com.socrata.soda.server.wiremodels.ComputationStrategyType
import com.socrata.soql.environment.{ColumnName, TypeName}

case class ResourceNotModified(override val etags: Seq[EntityTag],
                               override val vary: Option[String],
                               override val hasContent: Boolean = false)
  extends SodaError(SC_NOT_MODIFIED, "not-modified", "Resource was not modified")

case object EtagPreconditionFailed
  extends SodaError(SC_PRECONDITION_FAILED, "precondition-failed", "Precondition failed")

case object SchemaInvalidForMimeType
  extends SodaError(SC_NOT_ACCEPTABLE, "schema-invalid-for-mime-type", "Schema is invalid for given mime-type")

case class GeneralNotFoundError(path: String)
  extends SodaError(SC_NOT_FOUND, "not-found", s"Path not found: $path", "path" -> JString(path))

case class InternalError(tag: String, additionalData: (String, JValue)*)
  extends SodaError(SC_INTERNAL_SERVER_ERROR, "internal-error", s"Internal error: $tag; $additionalData",
                    Map("tag" -> JString(tag)) ++ additionalData)

case class InternalException(th: Throwable, tag: String)
  extends SodaError(SC_INTERNAL_SERVER_ERROR, "internal-error",
    s"Internal error: please include code $tag if you report the error",
    "tag"          -> JString(tag),
    "errorMessage" -> JString(Option(th.getMessage).getOrElse("")),
    "errorClass"   -> JString(th.getClass.getCanonicalName),
    "stackTrace"   -> JArray(th.getStackTrace.map(x => JString(x.toString)))
  ) {
  override def excludedFields = Set("errorMessage", "errorClass", "stackTrace")
}

case class HttpMethodNotAllowed(method: String, allowed: TraversableOnce[String])
  extends SodaError(SC_METHOD_NOT_ALLOWED, "method-not-allowed",
    s"HTTP method $method is not allowed for this request",
    "method" -> JString(method),
    "allowed" -> JsonEncode.toJValue(allowed.toSeq))

case object NoContentType extends SodaError("req.content-type.missing", "No content-type is set on the request")

// TODO change description to not use getOrElse when we make it non-optional
case class ErrorReportedByQueryCoordinator(code: Int, value: QueryCoordinatorError)
  extends SodaError(code, value.errorCode, s"Query coordinator error: ${value.errorCode}; ${value.description.getOrElse(value.errorCode)}", value.data)

case class UnparsableContentType(contentType: String)
  extends SodaError("req.content-type.unparsable", s"The content-type set on the request ($contentType) is unparseable",
    "content-type" -> JString(contentType))

case class ContentTypeNotJson(contentType: MimeType)
  extends SodaError(SC_UNSUPPORTED_MEDIA_TYPE, "req.content-type.not-json",
    s"The content-type set on the request (${contentType.toString}) should instead be of type 'application/json'",
    "content-type" -> JString(contentType.toString))

case class ContentTypeUnsupportedCharset(contentType: MimeType)
  extends SodaError(SC_UNSUPPORTED_MEDIA_TYPE, "req.content-type.unknown-charset",
    s"The content-type set on the request (${contentType.toString}) has an unsupported charset",
    "content-type" -> JString(contentType.toString))

case class MalformedJsonBody(row: Int, column: Int)
  extends SodaError("req.body.malformed-json", s"The json request body is malformed at row $row and column $column",
    "row" -> JNumber(row),
    "column" -> JNumber(column))

case class BodyTooLarge(limit: Long)
  extends SodaError(SC_REQUEST_ENTITY_TOO_LARGE, "req.body.too-large",
    s"The request body is too large; the limit is $limit bytes",
    "limit" -> JNumber(limit))

case class ContentNotSingleObject(value: JValue)
  extends SodaError("req.content.json.not-object",
    s"The json content of the request contains an unreadable object ($value)",
    "value" -> value)

case class DatasetSpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaError("soda.dataset.maltyped",
    s"Dataset is maltyped; for field '$field' we expected '$expected' but got '$got'",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class ColumnSpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaError("soda.column.maltyped",
    s"Column is maltyped; for field '$field' we expected '$expected' but got '$got'",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class ComputationStrategySpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaError("soda.column_computation_strategy.maltyped",
    s"Column computation strategy is maltyped; for field '$field' we expected '$expected' but got '$got'",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class ComputationStrategySpecUnknownType(typ: String)
  extends SodaError("soda.computation-strategy.unknown-type",
    s"The specified computation strategy type ($typ) is unknown",
    "type" -> JString(typ))

case class RollupSpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaError("soda.rollup.maltyped",
    s"Rollup is maltyped; for field '$field' we expected '$expected' but got '$got'",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class RollupCreationFailed(error: String)
  extends SodaError("soda.rollup.creation-failed", s"Rollup column creation failed due to $error",
    "error" -> JString(error))

case class RollupColumnNotFound(value: ColumnName)
  extends SodaError("soda.rollup.column-not-found", s"Rollup column '${value.name}' was not found",
    "value" -> JString(value.name))

case class RollupNotFound(value: RollupName)
  extends SodaError("soda.rollup.not-found", s"Rollup '${value.name}' was not found",
    "value" -> JString(value.name))

case class NonUniqueRowId(column: String)
  extends SodaError("soda.column.not-unique",
    s"Row-Identifier column '$column', which should contain unique values, instead contains duplicate values",
    "column" -> JString(column)
  )

case class ColumnSpecUnknownType(typeName: TypeName)
  extends SodaError("soda.column.unknown-type", s"Column is specified with unknown type ${typeName.name}",
    "type" -> JString(typeName.name))


// NOT FOUND

case class DatasetNotFound(value: ResourceName)
  extends SodaError(SC_NOT_FOUND, "soda.dataset.not-found", s"Dataset '${value.name}' was not found",
    "dataset" -> JString(value.name))

case class ColumnNotFound(resourceName: ResourceName, columnName: ColumnName)
  extends SodaError(SC_NOT_FOUND, "soda.dataset.column-not-found",
    s"Column ${columnName.name} was not found on dataset ${resourceName.name}",
    "dataset" -> JString(resourceName.name),
    "column"  -> JString(columnName.name))

case class RowNotFound(value: RowSpecifier)
  extends SodaError(SC_NOT_FOUND, "soda.row.not-found",
     s"Row with identifier ${value.underlying} was not found",
     "value" -> JString(value.underlying))

// BAD REQUESTS

case class BadParameter(param: String, value: String)
  extends SodaError(SC_BAD_REQUEST, "req.bad-parameter",
    s"Request has bad parameter '$param' with value '$value'",
    "parameter" -> JString(param),
    "value" -> JString(value))

case class ColumnHasDependencies(columnName: ColumnName,
                                 deps: Seq[ColumnName])
  extends SodaError(SC_BAD_REQUEST, "soda.column-with-dependencies-not-deleteable",
     s"Cannot delete column ${columnName.name} because it has dependent columns $deps which must be deleted first",
     "column"       -> JString(columnName.name),
     "dependencies" -> JArray(deps.map { d => JString(d.name) }))

case class DatasetAlreadyExistsSodaErr(dataset: ResourceName)
  extends SodaError(SC_BAD_REQUEST, "soda.dataset.already-exists",
    s"Dataset ${dataset.name} already exists",
    "dataset" -> JString(dataset.name))

case class DatasetNameInvalidNameSodaErr(dataset:  ResourceName)
  extends SodaError(SC_BAD_REQUEST, "soda.dataset.invalid-Name",
    s"Dataset name '${dataset.name}' is invalid",
    "dataset" -> JString(dataset.name))
/**
 * Column not found in a row operation
 */
case class RowColumnNotFound(value: ColumnName)
  extends SodaError(SC_BAD_REQUEST, "soda.row.column-not-found",
    s"One or more rows expected column '${value.name}' which was not found on the dataset",
    "value" -> JString(value.name))

case class ComputationHandlerNotFound(typ: ComputationStrategyType.Value)
  extends SodaError(SC_BAD_REQUEST, "soda.column.computation-handler-not-found",
    s"Computation handler not found for computation strategy type ${typ.toString}",
    "computation-strategy" -> JString(typ.toString))

case class NotAComputedColumn(value: ColumnName)
  extends SodaError(SC_BAD_REQUEST, "soda.column.not-a-computed-column",
    s"Column ${value.name} is not a computed column",
    "value" -> JString(value.name))

// internal intent is primary key; leaving the publicly-visible typing as "row-identifier"
case object CannotDeletePrimaryKey
  extends SodaError(SC_BAD_REQUEST, "soda.column.cannot-delete-row-identifier",
    s"Cannot delete the column which is currently defined as the row-identifier")

// guessing as to the intended purpose of this one; there's nothing that uses this at the moment
case object DeleteWithoutPrimaryKey
  extends SodaError(SC_BAD_REQUEST, "soda.row.delete-without-primary-key",
    s"Cannot delete an individual row without giving the primary key/row-identifier value")

case class UpsertRowNotAnObject(obj: JValue)
  extends SodaError(SC_BAD_REQUEST, "soda.row.upsert-row-not-an-object",
    s"Row is not parseable as a json object: $obj",
    "value" -> obj)

// value here seems pretty useless; haven't seen it set to anything useful, at least
case class RowPrimaryKeyNonexistentOrNull(value: RowSpecifier)
  extends SodaError(SC_BAD_REQUEST, "soda.row.primary-key-nonexistent-or-null",
    s"Row in dataset lacks a primary key or has the primary key column set to null",
    "value" -> JString(value.underlying))

case class DatasetWriteLockError(dataset: ResourceName)
  extends SodaError(SC_CONFLICT, "soda.dataset.cannot-acquire-write-lock",
    s"Dataset ${dataset.name} cannot acquire a write lock",
    "dataset" -> JString(dataset.name))

case class LocaleChangedError(locale: String)
  extends SodaError(SC_CONFLICT, "soda.dataset.locale-changed",
    s"Dataset locale is different from the default locale of $locale",
    "locale" -> JString(locale))

case class UnsupportedUpdateOperation(errMessage: String)
  extends SodaError(SC_BAD_REQUEST, "soda.unsupported-update-operation",
    s"Requested Update Operation is not supported.",
    "message" -> JString(errMessage))
