package com.socrata.soda.clients.querycoordinator

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.ast.{JObject, JValue}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, NoTag, SimpleHierarchyCodecBuilder, TagAndValue}
import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soda.server.macros.{BranchCodec, Tag}



// Query Errors
@Tag("query.datasource.unavailable")
case class DataSourceUnavailable(datasetId: DatasetId) extends QueryError

@Tag("query.dataset.does-not-exist")
case class DoesNotExist(datasetId: DatasetId) extends QueryError

// Request Errors
@Tag("req.no-dataset-specified")
case class NoDatasetSpecified() extends RequestError

@Tag("req.no-query-specified")
case class NoQuerySpecified() extends RequestError

@Tag("req.unknown.column-ids")
case class UnknownColumnIds(columns: Seq[ColumnId]) extends RequestError

@Tag("req.row-limit-exceeded")
case class RowLimitExceeded(limit: Long) extends RequestError


// SOQL Errors
// TODO make parameters of each item specific if need be.

@Tag("query.soql.aggregate-in-ungrouped-context")
case class AggregateInUngroupedContext(data: JObject) extends SoqlError

@Tag("query.soql.column-not-in-group-bys")
case class ColumnNotInGroupBys(data: JObject) extends SoqlError

@Tag("query.soql.repeated-exception")
case class RepeatedException(data: JObject) extends SoqlError

@Tag("query.soql.duplicate-alias")
case class DuplicateAlias(data: JObject) extends SoqlError

@Tag("query.soql.no-such-column")
case class NoSuchColumn(data: JObject) extends SoqlError

@Tag("query.soql.circular-alias-definition")
case class CircularAliasDefinition(data: JObject) extends SoqlError

@Tag("query.soql.unexpected-escape")
case class UnexpectedEscape(data: JObject) extends SoqlError

@Tag("query.soql.bad-unicode-escape-character")
case class BadUnicodeEscapeCharacter(data: JObject) extends SoqlError

@Tag("query.soql.unicode-character-out-of-range")
case class UnicodeCharacterOutOfRange(data: JObject) extends SoqlError

@Tag("query.soql.unexpected-character")
case class UnexpectedCharacter(data: JObject) extends SoqlError

@Tag("query.soql.unexpected-eof")
case class UnexpectedEOF(data: JObject) extends SoqlError

@Tag("query.soql.unterminated-string")
case class UnterminatedString(data: JObject) extends SoqlError

@Tag("query.soql.bad-parse")
case class BadParse(data: JObject) extends SoqlError

@Tag("query.soql.no-such-function")
case class NoSuchFunction(data: JObject) extends SoqlError

@Tag("query.soql.type-mismatch")
case class TypeMismatch(data: JObject) extends SoqlError


@Tag("query.soql.ambiguous-call")
case class AmbiguousCall(data: JObject) extends SoqlError


sealed abstract class SoqlError extends PossiblyUnknownQueryCoordinatorError {
  def code: String = getClass.getAnnotation(classOf[Tag]).value()
}
object SoqlError {
  implicit val jCodec = BranchCodec(SimpleHierarchyCodecBuilder[SoqlError](TagAndValue("errorCode", "data"))).build
}


sealed abstract class QueryError extends PossiblyUnknownQueryCoordinatorError {
  def code: String = getClass.getAnnotation(classOf[Tag]).value()
}
object QueryError {
  implicit val jCodec = BranchCodec(SimpleHierarchyCodecBuilder[QueryError](TagAndValue("errorCode", "data"))).build
}


sealed abstract class RequestError extends PossiblyUnknownQueryCoordinatorError {
  def code: String = getClass.getAnnotation(classOf[Tag]).value()
}
object RequestError {
  implicit val jCodec = BranchCodec(SimpleHierarchyCodecBuilder[RequestError](TagAndValue("errorCode", "data"))).build
}

// TODO after this hits all the production envs, make description non-optional (only making it optional to avoid mis-classification during release of sf+qc)
case class UnknownQueryCoordinatorError(errorCode: String, description: Option[String], data: Map[String, JValue]) extends PossiblyUnknownQueryCoordinatorError
object UnknownQueryCoordinatorError {
  implicit val jCodec = AutomaticJsonCodecBuilder[UnknownQueryCoordinatorError]
}


sealed abstract class PossiblyUnknownQueryCoordinatorError
object PossiblyUnknownDataCoordinatorError {
  implicit val jCodec = SimpleHierarchyCodecBuilder[PossiblyUnknownQueryCoordinatorError](NoTag).
    branch[SoqlError].
    branch[QueryError].
    branch[UnknownQueryCoordinatorError].
    build
}

// TODO after this hits all the production envs, make description non-optional (only making it optional to avoid mis-classification during release of sf+qc)
case class QueryCoordinatorError(errorCode: String, description: Option[String], data: Map[String, JValue])
object QueryCoordinatorError{
  implicit val jCodec = AutomaticJsonCodecBuilder[QueryCoordinatorError]
}



