package com.socrata.soda.server.highlevel

import com.rojoma.json.v3.ast.{JObject, JValue}
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.http.server.util.RequestId.RequestId
import com.socrata.soda.server.highlevel.ColumnSpecUtils.CreateResult
import com.socrata.soda.server.highlevel.DatasetDAO.SuccessResult
import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.persistence.{ColumnRecord, DatasetRecord}
import com.socrata.soda.server.wiremodels.UserProvidedColumnSpec
import com.socrata.soql.environment.ColumnName

trait ColumnDAO {
  import ColumnDAO.Result

  def replaceOrCreateColumn(user: String,
                            dataset: ResourceName,
                            precondition: Precondition,
                            expectedDataVersion: Option[Long],
                            column: ColumnName,
                            spec: UserProvidedColumnSpec,
                            requestId: RequestId): Result

  def updateColumn(user: String, dataset: ResourceName, expectedDataVersion: Option[Long], column: ColumnName, spec: UserProvidedColumnSpec, requestId: RequestId): Result

  def deleteColumn(user: String, dataset: ResourceName, expectedDataVersion: Option[Long], column: ColumnName, requestId: RequestId): Result

  def makePK(user: String, dataset: ResourceName, expectedDataVersion: Option[Long], column: ColumnName): Result

  def createOrUpdateIndexDirective(user: String, dataset: ResourceName, expectedDataVersion: Option[Long], column: ColumnName, directive: JObject, requestId: RequestId): Result

  def dropIndexDirectives(user: String, dataset: ResourceName, expectedDataVersion: Option[Long], column: ColumnName, requestId: RequestId): Result

  def getColumn(dataset: ResourceName, column: ColumnName): Result
}

object ColumnDAO {
  sealed abstract class Result
  sealed abstract class SuccessResult extends Result
  sealed abstract class UpdateSuccessResult extends SuccessResult
  sealed abstract class FailResult extends Result

  // SUCCESS
  case class Created(columnRec: ColumnRecord, etag: Option[EntityTag]) extends UpdateSuccessResult
  case class Updated(columnRec: ColumnRecord, etag: Option[EntityTag]) extends UpdateSuccessResult
  case class Found(datasetRec: DatasetRecord, columnRec: ColumnRecord, etag: Option[EntityTag]) extends SuccessResult
  case class Deleted(rec: ColumnRecord, etag: Option[EntityTag]) extends SuccessResult
  case object EmptyResult extends SuccessResult

  // FAILURES: DataCoordinator
  case class ColumnAlreadyExists(columnName: ColumnName) extends FailResult
  case class IllegalColumnId(columnName: ColumnName) extends FailResult
  case class InvalidSystemColumnOperation(columnName: ColumnName) extends FailResult
  case class ColumnNotFound(columnName: ColumnName) extends FailResult
  case class DuplicateValuesInColumn(rec: ColumnRecord) extends FailResult
  case class InternalServerError(code: String, tag: String, data: String) extends FailResult
  case class CannotDeleteRowId(columnRec: ColumnRecord, method: String) extends FailResult
  case class DatasetNotFound(dataset: ResourceName) extends FailResult
  case class DatasetVersionMismatch(dataset: ResourceName, version: Long) extends FailResult
  case object CannotChangeColumnId extends FailResult
  case object CannotChangeColumnType extends FailResult

  case class ComputationStrategyValidationError(error: com.socrata.computation_strategies.ValidationError) extends FailResult
  case class ComputationStrategyValidationErrorResult(result: CreateResult) extends FailResult

  // FAILURES: Internally consumed only
  case class InvalidColumnName(col: ColumnName) extends FailResult
  case class ColumnHasDependencies(col: ColumnName, deps: Seq[ColumnName]) extends FailResult
  case class PreconditionFailed(reason: Precondition.Failure) extends FailResult

}
