package com.socrata.soda.server.responses

import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse._

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec._
import com.socrata.computation_strategies.StrategyType
import com.socrata.http.server.util.EntityTag
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorError
import com.socrata.soda.server.id.{IndexName, ResourceName, RollupName, RowSpecifier}
import com.socrata.soql.environment.{ColumnName, TypeName}

case class ResourceNotModified(override val etags: Seq[EntityTag],
                               override val vary: Option[String],
                               override val hasContent: Boolean = false)
  extends SodaResponse(SC_NOT_MODIFIED, "not-modified", "Resource was not modified")

case object EtagPreconditionFailed
  extends SodaResponse(SC_PRECONDITION_FAILED, "precondition-failed", "Precondition failed")

case class RequestTimedOut(customTimeout: JValue)
  extends SodaResponse(SC_REQUEST_TIMEOUT, "request-timeout", "Request timed out", "customTimeout" -> customTimeout)

case object SchemaInvalidForMimeType
  extends SodaResponse(SC_NOT_ACCEPTABLE, "schema-invalid-for-mime-type", "Schema is invalid for given mime-type")

case class GeneralNotFoundError(path: String)
  extends SodaResponse(SC_NOT_FOUND, "not-found", s"Path not found: $path", "path" -> JString(path))

case class InternalError(tag: String, additionalData: (String, JValue)*)
  extends SodaResponse(SC_INTERNAL_SERVER_ERROR, "internal-error", s"Internal error: $tag; $additionalData",
                    Map("tag" -> JString(tag)) ++ additionalData)

case class InternalException(th: Throwable, tag: String)
  extends SodaResponse(SC_INTERNAL_SERVER_ERROR, "internal-error",
    s"Internal error: please include code $tag if you report the error",
    "tag"          -> JString(tag),
    "errorMessage" -> JString(Option(th.getMessage).getOrElse("")),
    "errorClass"   -> JString(th.getClass.getCanonicalName),
    "stackTrace"   -> JArray(th.getStackTrace.map(x => JString(x.toString)))
  ) {
  override def excludedFields = Set("errorMessage", "errorClass", "stackTrace")
}

case object ServiceUnavailable
  extends SodaResponse(SC_SERVICE_UNAVAILABLE, "service-unavailable",
    "Service unavailable")

case object TooManyRequests
  extends SodaResponse(429, "too-many-requests",
      "Too many requests")

case class SodaInvalidRequest(th: Throwable, tag: String)
  extends SodaResponse(SC_BAD_REQUEST, "invalid-request",
    s"Invalid request: ${Option(th.getMessage).getOrElse("")}",
    "errorMessage" -> JString(Option(th.getMessage).getOrElse("")),
    "errorClass"   -> JString(th.getClass.getCanonicalName),
    "stackTrace"   -> JArray(th.getStackTrace.map(x => JString(x.toString)))
  ) {
  override def excludedFields = Set("errorMessage", "errorClass", "stackTrace")
}

case class HttpMethodNotAllowed(method: String, allowed: TraversableOnce[String])
  extends SodaResponse(SC_METHOD_NOT_ALLOWED, "method-not-allowed",
    s"HTTP method $method is not allowed for this request",
    "method" -> JString(method),
    "allowed" -> JsonEncode.toJValue(allowed.toSeq))

case object NoContentType extends SodaResponse("req.content-type.missing", "No content-type is set on the request")

case class ErrorReportedByQueryCoordinator(code: Int, value: QueryCoordinatorError)
  extends SodaResponse(code, value.errorCode, s"Query coordinator error: ${value.errorCode}; ${value.description}", value.data)

case class UnparsableContentType(contentType: String)
  extends SodaResponse("req.content-type.unparsable", s"The content-type set on the request ($contentType) is unparseable",
    "content-type" -> JString(contentType))

case class ContentTypeNotJson(contentType: MimeType)
  extends SodaResponse(SC_UNSUPPORTED_MEDIA_TYPE, "req.content-type.not-json",
    s"The content-type set on the request (${contentType.toString}) should instead be of type 'application/json'",
    "content-type" -> JString(contentType.toString))

case class ContentTypeUnsupportedCharset(contentType: MimeType)
  extends SodaResponse(SC_UNSUPPORTED_MEDIA_TYPE, "req.content-type.unknown-charset",
    s"The content-type set on the request (${contentType.toString}) has an unsupported charset",
    "content-type" -> JString(contentType.toString))

case class MalformedJsonBody(row: Int, column: Int)
  extends SodaResponse("req.body.malformed-json", s"The json request body is malformed at row $row and column $column",
    "row" -> JNumber(row),
    "column" -> JNumber(column))

case class BodyTooLarge(limit: Long)
  extends SodaResponse(SC_REQUEST_ENTITY_TOO_LARGE, "req.body.too-large",
    s"The request body is too large; the limit is $limit bytes",
    "limit" -> JNumber(limit))

case class ContentNotSingleObject(value: JValue)
  extends SodaResponse("req.content.json.not-object",
    s"The json content of the request contains an unreadable object ($value)",
    "value" -> value)

case class InvalidJsonContent(expected: String)
  extends SodaResponse("req.content.json.invalid",
    s"The json content of the request is not an array or object",
    "expected" -> JString(expected))

case class DatasetSpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaResponse("soda.dataset.maltyped",
    s"Dataset is maltyped; for field '$field' we expected '$expected' but got '$got'",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class ColumnSpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaResponse("soda.column.maltyped",
    s"Column is maltyped; for field '$field' we expected '$expected' but got '$got'",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class ComputationStrategySpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaResponse("soda.column_computation_strategy.maltyped",
    s"Column computation strategy is maltyped; for field '$field' we expected '$expected' but got '$got'",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class ComputationStrategySpecUnknownType(typ: String)
  extends SodaResponse("soda.computation-strategy.unknown-type",
    s"The specified computation strategy type ($typ) is unknown",
    "type" -> JString(typ))

case class RollupSpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaResponse("soda.rollup.maltyped",
    s"Rollup is maltyped; for field '$field' we expected '$expected' but got '$got'",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class RollupCreationFailed(error: String)
  extends SodaResponse("soda.rollup.creation-failed", s"Rollup column creation failed due to $error",
    "error" -> JString(error))

case class RollupColumnNotFound(value: ColumnName)
  extends SodaResponse("soda.rollup.column-not-found", s"Rollup column '${value.name}' was not found",
    "value" -> JString(value.name))

case class RollupNotFound(value: RollupName)
  extends SodaResponse("soda.rollup.not-found", s"Rollup '${value.name}' was not found",
    "value" -> JString(value.name))

case class IndexSpecMaltyped(field: String, expected: String, got: JValue)
  extends SodaResponse("soda.index.maltyped",
    s"Index is maltyped; for field '$field' we expected '$expected' but got '$got'",
    "field" -> JString(field),
    "expected" -> JString(expected),
    "got" -> got)

case class IndexNotFound(value: IndexName)
  extends SodaResponse("soda.index.not-found", s"Index '${value.name}' was not found",
    "value" -> JString(value.name))

case class IndexCreationFailed(error: String)
  extends SodaResponse("soda.index.creation-failed", s"Index creation failed due to $error",
    "error" -> JString(error))

case class NonUniqueRowId(column: ColumnName)
  extends SodaResponse("soda.column.not-unique",
    s"Row-Identifier column '${column.name}', which should contain unique values, instead contains duplicate values",
    "column" -> JString(column.name)
  )

case class ColumnSpecUnknownType(typeName: TypeName)
  extends SodaResponse("soda.column.unknown-type", s"Column is specified with unknown type ${typeName.name}",
    "type" -> JString(typeName.name))


// NOT FOUND

case class DatasetNotFound(value: ResourceName)
  extends SodaResponse(SC_NOT_FOUND, "soda.dataset.not-found", s"Dataset '${value.name}' was not found",
    "dataset" -> JString(value.name))

case class ColumnNotFound(resourceName: ResourceName, columnName: ColumnName)
  extends SodaResponse(SC_NOT_FOUND, "soda.dataset.column-not-found",
    s"Column ${columnName.name} was not found on dataset ${resourceName.name}",
    "dataset" -> JString(resourceName.name),
    "column"  -> JString(columnName.name))

case class RowNotFound(value: RowSpecifier)
  extends SodaResponse(SC_NOT_FOUND, "soda.row.not-found",
     s"Row with identifier ${value.underlying} was not found",
     "value" -> JString(value.underlying))

case class SnapshotNotFound(value: ResourceName, snapshot: Long)
  extends SodaResponse(SC_NOT_FOUND, "soda.snapshot.not-found", s"Snapshot ${snapshot} for dataset '${value.name}' was not found",
    "dataset" -> JString(value.name),
    "snapshot" -> JNumber(snapshot))

case class DatasetNotInSecondary(secondary: String, dataset: ResourceName)
  extends SodaResponse(SC_NOT_FOUND, "req.secondary",
    s"Dataset with name '${dataset.name}' is not in secondary with id '$secondary'",
    "secondary_id" -> JString(secondary),
    "dataset" -> JString(dataset.name)
)

// BAD REQUESTS

case class BadParameter(param: String, value: String)
  extends SodaResponse(SC_BAD_REQUEST, "req.bad-parameter",
    s"Request has bad parameter '$param' with value '$value'",
    "parameter" -> JString(param),
    "value" -> JString(value))

case class ColumnHasDependencies(columnName: ColumnName,
                                 deps: Seq[ColumnName])
  extends SodaResponse(SC_BAD_REQUEST, "soda.column-with-dependencies-not-deleteable",
     s"Cannot delete column ${columnName.name} because it has dependent columns $deps which must be deleted first",
     "column"       -> JString(columnName.name),
     "dependencies" -> JArray(deps.map { d => JString(d.name) }))

case class DatasetAlreadyExistsSodaErr(dataset: ResourceName)
  extends SodaResponse(SC_BAD_REQUEST, "soda.dataset.already-exists",
    s"Dataset ${dataset.name} already exists",
    "dataset" -> JString(dataset.name))

case class DatasetNameInvalidNameSodaErr(dataset:  ResourceName)
  extends SodaResponse(SC_BAD_REQUEST, "soda.dataset.invalid-Name",
    s"Dataset name '${dataset.name}' is invalid",
    "dataset" -> JString(dataset.name))

case class DatasetVersionMismatch(dataset: ResourceName, version: Long)
  extends SodaResponse(SC_CONFLICT, "soda.row.dataset-version-mismatch",
    s"The dataset does not have the required version",
    "dataset" -> JString(dataset.name),
    "version" -> JNumber(version))

/**
 * Column not found in a row operation
 */
case class RowColumnNotFound(value: ColumnName)
  extends SodaResponse(SC_BAD_REQUEST, "soda.row.column-not-found",
    s"One or more rows expected column '${value.name}' which was not found on the dataset",
    "value" -> JString(value.name))

case class ComputationHandlerNotFound(typ: StrategyType)
  extends SodaResponse(SC_BAD_REQUEST, "soda.column.computation-handler-not-found",
    s"Computation handler not found for computation strategy type ${typ.toString}",
    "computation-strategy" -> JString(typ.toString))

case class NotAComputedColumn(value: ColumnName)
  extends SodaResponse(SC_BAD_REQUEST, "soda.column.not-a-computed-column",
    s"Column ${value.name} is not a computed column",
    "value" -> JString(value.name))

// internal intent is primary key; leaving the publicly-visible typing as "row-identifier"
case object CannotDeletePrimaryKey
  extends SodaResponse(SC_BAD_REQUEST, "soda.column.cannot-delete-row-identifier",
    s"Cannot delete the column which is currently defined as the row-identifier")

// guessing as to the intended purpose of this one; there's nothing that uses this at the moment
case object DeleteWithoutPrimaryKey
  extends SodaResponse(SC_BAD_REQUEST, "soda.row.delete-without-primary-key",
    s"Cannot delete an individual row without giving the primary key/row-identifier value")

case class UpsertRowNotAnObject(obj: JValue)
  extends SodaResponse(SC_BAD_REQUEST, "soda.row.upsert-row-not-an-object",
    s"Row is not parseable as a json object: $obj",
    "value" -> obj)

// value here seems pretty useless; haven't seen it set to anything useful, at least
case class RowPrimaryKeyNonexistentOrNull(value: RowSpecifier)
  extends SodaResponse(SC_BAD_REQUEST, "soda.row.primary-key-nonexistent-or-null",
    s"Row in dataset lacks a primary key or has the primary key column set to null",
    "value" -> JString(value.underlying))

case class DatasetWriteLockError(dataset: ResourceName)
  extends SodaResponse(SC_CONFLICT, "soda.dataset.cannot-acquire-write-lock",
    s"Dataset ${dataset.name} cannot acquire a write lock",
    "dataset" -> JString(dataset.name))

case class FeedbackInProgressError(dataset: ResourceName, stores: Set[String])
  extends SodaResponse(SC_CONFLICT, "soda.dataset.feedback-in-progress",
    s"Dataset ${dataset.name} is undergoing asynchronous processing; publication stage cannot be changed",
    "dataset" -> JString(dataset.name),
    "stores" -> JsonEncode.toJValue(stores))

case class IncorrectLifecycleStage(actualStage: String, expectedStage: Set[String])
  extends SodaResponse(SC_CONFLICT,
                       "soda.dataset.incorrect-lifecycle-stage",
                       "Dataset is in incorrect lifecycle stage",
                       "actual" -> JString(actualStage),
                       "expected" -> JArray(expectedStage.toSeq.map(JString(_))))

case class LocaleChangedError(locale: String)
  extends SodaResponse(SC_CONFLICT, "soda.dataset.locale-changed",
    s"Dataset locale is different from the default locale of $locale",
    "locale" -> JString(locale))

case class UnsupportedUpdateOperation(errMessage: String)
  extends SodaResponse(SC_BAD_REQUEST, "soda.unsupported-update-operation",
    s"Requested Update Operation is not supported.",
    "message" -> JString(errMessage))

case object InvalidRowId
  extends SodaResponse(SC_BAD_REQUEST, "soda.invalid-row-identifier",
    s"Row-identifier is invalid")

case class CollocateError(msg: String)
  extends SodaResponse(SC_BAD_REQUEST, "soda.collocate-error", //Does this string need to be referring to something in a different project?
      s"Collocate failed with: $msg")
