package com.socrata.soda.clients.querycoordinator

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.{ColumnId, DatasetHandle}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.stdlib.Context
import org.joda.time.DateTime

object QueryCoordinatorClient {
  val client = "QC"

  sealed abstract class Result
  sealed abstract class SuccessResult extends Result
  sealed abstract class FailResult extends Result

  // Success Cases
  case class Success(etags: Seq[EntityTag], rollup: Option[String], lastModified: String, response: Iterator[JValue]) extends SuccessResult

  // Fail Cases
  case class NotModified(etags: Seq[EntityTag]) extends FailResult
  case object PreconditionFailed extends FailResult
  case object ServiceUnavailable extends FailResult
  case object TooManyRequests extends FailResult
  case class RequestTimedOut(timeout: JValue) extends FailResult
  object RequestTimedOut {
    implicit val jCodec = AutomaticJsonCodecBuilder[RequestTimedOut]
  }

  case class QueryCoordinatorResult(status: Int,  payload: QueryCoordinatorError) extends FailResult

  // TODO do we want the granularity provided below or not; if so machinery is left in tact to do so but can
  // be removed after a few commits.

  // Request Errors
//  case class DataSourceUnavailableResult(code: String, datasetId: DatasetId) extends FailResult
//  case class DoesNotExistResult(code: String, datasetId: DatasetId) extends FailResult
//
//  // Query Errors
//  case class NoDatasetSpecifiedResult(code: String) extends FailResult
//  case class NoQuerySpecifiedResult(code: String) extends FailResult
//  case class UnknownColumnIdsResult(code: String, columns: Seq[ColumnId]) extends FailResult
//  case class RowLimitExceededResult(code: String, limit: Long) extends FailResult
//
//  // Soql Errors
//  case class AggregateInUngroupedContextResult(code: String, data: JObject) extends FailResult
//  case class ColumnNotInGroupBysResult(code: String, data: JObject) extends FailResult
//  case class RepeatedExceptionResult(code: String, data: JObject) extends FailResult
//  case class DuplicateAliasResult(code: String, data: JObject) extends FailResult
//  case class NoSuchColumnResult(code: String, data: JObject) extends FailResult
//  case class CircularAliasDefinitionResult(code: String, data: JObject) extends FailResult
//  case class UnexpectedEscapeResult(code: String, data: JObject) extends FailResult
//  case class BadUnicodeEscapeCharacterResult(code: String, data: JObject) extends FailResult
//  case class UnicodeCharacterOutOfRangeResult(code: String, data: JObject) extends FailResult
//  case class UnexpectedCharacterResult(code: String, data: JObject) extends FailResult
//  case class UnexpectedEOFResult(code: String, data: JObject) extends FailResult
//  case class UnterminatedStringResult(code: String, data: JObject) extends FailResult
//  case class BadParseResult(code: String, data: JObject) extends FailResult
//  case class NoSuchFunctionResult(code: String, data: JObject) extends FailResult
//  case class TypeMismatchResult(code: String, data: JObject) extends FailResult
//  case class AmbiguousCallResult(code: String, data: JObject) extends FailResult

  case class InternalServerErrorResult(status: Int, code: String, tag: String, data: String) extends FailResult


  val HeaderRollup = "X-SODA2-Rollup"

}

trait QueryCoordinatorClient {
  import QueryCoordinatorClient._
  def query[T](dataset: DatasetHandle,
               precondition: Precondition,
               ifModifiedSince: Option[DateTime],
               query: String,
               context: Context,
               rowCount: Option[String],
               copy: Option[Stage],
               secondaryInstance: Option[String],
               noRollup: Boolean,
               obfuscateId: Boolean,
               extraHeaders: Map[String, String],
               queryTimeoutSeconds: Option[String],
               rs: ResourceScope,
               lensUid: Option[String])(f: Result => T): T

}
