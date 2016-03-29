package com.socrata.soda.server.highlevel

import com.rojoma.json.v3.ast.JValue
import com.socrata.http.server.util.RequestId.RequestId
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.{SecondaryVersionsReport, VersionReport}
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.{RollupName, SecondaryId, ResourceName}
import com.socrata.soda.server.persistence.DatasetRecord
import com.socrata.soda.server.wiremodels.{UserProvidedRollupSpec, UserProvidedDatasetSpec}
import com.socrata.soql.environment.ColumnName

trait DatasetDAO {
  import DatasetDAO.Result
  def createDataset(user: String, spec: UserProvidedDatasetSpec, requestId: RequestId): Result
  def replaceOrCreateDataset(user: String,
                             dataset: ResourceName,
                             spec: UserProvidedDatasetSpec,
                             requestId: RequestId): Result
  def updateDataset(user: String,
                    dataset: ResourceName,
                    spec: UserProvidedDatasetSpec,
                    requestId: RequestId): Result
  def markDatasetForDeletion(user: String, dataset: ResourceName): Result
  def removeDataset(user: String, dataset: ResourceName, requestId: RequestId):Result
  def getDataset(dataset: ResourceName, stage: Option[Stage]): Result
  def getSecondaryVersions(dataset: ResourceName, requestId: RequestId): Result
  def getVersion(dataset: ResourceName, secondary: SecondaryId, requestId: RequestId): Result
  def getCurrentCopyNum(dataset: ResourceName): Option[Long]

  def makeCopy(user: String, dataset: ResourceName, copyData: Boolean, requestId: RequestId): Result
  def dropCurrentWorkingCopy(user: String, dataset: ResourceName, requestId: RequestId): Result
  def publish(user: String, dataset: ResourceName, snapshotLimit: Option[Int], requestId: RequestId): Result
  def propagateToSecondary(dataset: ResourceName, secondary: SecondaryId, requestId: RequestId): Result

  def replaceOrCreateRollup(user: String,
                            dataset: ResourceName,
                            rollup: RollupName,
                            spec: UserProvidedRollupSpec,
                            requestId: RequestId): Result
  def getRollup(user: String, dataset: ResourceName, rollup: RollupName, requestId: RequestId): Result
  def deleteRollup(user: String, dataset: ResourceName, rollup: RollupName, requestId: RequestId): Result
}

object DatasetDAO {
  sealed abstract class Result
  sealed abstract class SuccessResult extends Result
  sealed abstract class FailResult extends Result

  // SUCCESS
  case class Created(dataset: DatasetRecord) extends SuccessResult
  case class Updated(dataset: DatasetRecord) extends SuccessResult
  case class Found(dataset: DatasetRecord) extends SuccessResult
  case class DatasetSecondaryVersions(versions: SecondaryVersionsReport) extends SuccessResult
  case class DatasetVersion(version: VersionReport) extends SuccessResult
  case object Deleted extends SuccessResult
  case object WorkingCopyCreated extends SuccessResult
  case object WorkingCopyDropped extends SuccessResult
  case object WorkingCopyPublished extends SuccessResult
  case object PropagatedToSecondary extends SuccessResult
  case object RollupCreatedOrUpdated extends SuccessResult
  case object RollupDropped extends SuccessResult

  // FAILURES: DataCoordinator
  case class RollupNotFound(name: RollupName) extends FailResult
  case class DatasetNotFound(name: ResourceName) extends FailResult
  case class CannotAcquireDatasetWriteLock(name: ResourceName) extends FailResult
  case class FeedbackInProgress(name: ResourceName, stores: Set[String]) extends FailResult
  case class InternalServerError(code: String, tag: String, data: String) extends FailResult
  case class UnexpectedInternalServerResponse(reason: String, tag: String) extends FailResult

  // FAILURES: Internally consumed
  case class InvalidDatasetName(name: ResourceName) extends FailResult
  case class NonExistentColumn(dataset: ResourceName, name: ColumnName) extends FailResult
  case class DatasetAlreadyExists(name: ResourceName) extends FailResult
  case class LocaleChanged(locale: String) extends FailResult
  case class RollupError(message: String) extends FailResult
  case class RollupColumnNotFound(column: ColumnName) extends FailResult
  case class UnsupportedUpdateOperation(message: String) extends FailResult
}
