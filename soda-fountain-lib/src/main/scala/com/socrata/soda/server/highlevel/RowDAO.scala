package com.socrata.soda.server.highlevel

import com.rojoma.json.v3.ast.JValue
import com.socrata.computation_strategies.StrategyType
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.http.server.util.RequestId.RequestId
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.ReportItem
import com.socrata.soda.clients.datacoordinator.{RowUpdate, RowUpdateOption}
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorError
import com.socrata.soda.server.id.{RowSpecifier, ResourceName}
import com.socrata.soda.server.persistence.DatasetRecordLike
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.stdlib.Context
import com.socrata.soql.types.{SoQLValue, SoQLType}
import org.joda.time.DateTime

import RowDAO._
import com.socrata.soda.server.copy.Stage
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.soda.server.resources.DebugInfo

trait RowDAO {
  def query(dataset: ResourceName,
            precondition: Precondition,
            ifModifiedSince: Option[DateTime],
            query: String,
            context: Context,
            rowCount: Option[String],
            stage: Option[Stage],
            secondaryInstance: Option[String],
            noRollup: Boolean,
            obfuscateId: Boolean,
            fuseColumns: Option[String],
            queryTimeoutSeconds: Option[String],
            debugInfo: DebugInfo,
            resourceScope: ResourceScope,
            lensUid: Option[String]): Result

  def getRow(dataset: ResourceName,
             precondition: Precondition,
             ifModifiedSince: Option[DateTime],
             rowId: RowSpecifier,
             stage: Option[Stage],
             secondaryInstance:Option[String],
             noRollup: Boolean,
             obfuscateId: Boolean,
             fuseColumns: Option[String],
             queryTimeoutSeconds: Option[String],
             debugInfo: DebugInfo,
             resourceScope: ResourceScope,
             lensUid: Option[String]): Result

  def upsert[T](user: String,
                datasetRecord: DatasetRecordLike,
                expectedDataVersion: Option[Long],
                data: Iterator[RowUpdate],
                rowUpdateOption: RowUpdateOption = RowUpdateOption.default)
               (f: UpsertResult => T): T

  def replace[T](user: String, datasetRecord: DatasetRecordLike, expectedDataVersion: Option[Long], data: Iterator[RowUpdate])
                (f: UpsertResult => T): T

  def deleteRow[T](user: String, dataset: ResourceName, expectedDataVersion: Option[Long], rowId: RowSpecifier)
                  (f: UpsertResult => T): T
}

object RowDAO {
  sealed abstract class Result
  sealed abstract class SuccessResult extends Result
  sealed abstract class FailResult extends Result

  // SUCCESS
  case class Success(status: Int, body: JValue) extends SuccessResult
  case class QuerySuccess(etags: Seq[EntityTag], truthVersion: Long, truthLastModified: DateTime,
                          rollup: Option[String], schema: ExportDAO.CSchema,
                          body: Iterator[Array[SoQLValue]]) extends SuccessResult
  case class SingleRowQuerySuccess(etags: Seq[EntityTag], truthVersion: Long, truthLastModified: DateTime,
                                   schema: ExportDAO.CSchema, body: Array[SoQLValue]) extends SuccessResult
  case class InfoSuccess(status: Int, body: Iterator[JValue]) extends SuccessResult

  // FAILURE: QueryCoordinator
  case class PreconditionFailed(failure: Precondition.Failure) extends FailResult
  case class RequestTimedOut(timeout: JValue) extends FailResult
  case object ServiceUnavailable extends FailResult
  case object TooManyRequests extends FailResult

  // FAILURES: Internally consumed
  case object TooManyRows extends FailResult
  case object SchemaInvalidForMimeType extends FailResult
  case class MaltypedData(column: ColumnName, expected: SoQLType, got: JValue) extends UpsertFailResult


  sealed trait UpsertResult
  sealed trait UpsertSuccessResult extends SuccessResult with UpsertResult
  sealed trait UpsertFailResult extends FailResult with UpsertResult

  // UPSERT SUCCESS
  case class StreamSuccess(report: Iterator[ReportItem], newDataVersion: Long, newDataShapeVersion: Long) extends UpsertSuccessResult

  // UPSERT FAILURE: DataCoordinator
  case object SchemaOutOfSync extends UpsertFailResult
  case class RowNotFound(specifier: RowSpecifier) extends UpsertFailResult
  case class RowPrimaryKeyIsNonexistentOrNull(specifier: RowSpecifier) extends UpsertFailResult
  case class DatasetNotFound(dataset: ResourceName) extends UpsertFailResult
  case class UnknownColumn(column: ColumnName) extends UpsertFailResult
  case object CannotDeletePrimaryKey extends UpsertFailResult
  case object DeleteWithoutPrimaryKey extends UpsertFailResult
  case class InvalidRequest(client: String, status: Int, body: JValue) extends UpsertFailResult
  case class RowNotAnObject(value: JValue) extends UpsertFailResult
  case class DatasetVersionMismatch(dataset: ResourceName, version: Long) extends UpsertFailResult
  case class InternalServerError(status: Int = 500, client: String = "QC", code: String, tag: String, data: String) extends UpsertFailResult

  // UPSERT FAILURE: QueryCoordinator
  case class QCError(status: Int, error: QueryCoordinatorError) extends UpsertFailResult

  // UPSERT FAILURE: UNKNOWN
  case class ComputationHandlerNotFound(typ: StrategyType) extends UpsertFailResult
}
