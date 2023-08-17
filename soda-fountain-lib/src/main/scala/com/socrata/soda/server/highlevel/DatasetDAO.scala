package com.socrata.soda.server.highlevel

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}
import com.socrata.http.server.util.RequestId.RequestId
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.{SecondaryVersionsReport, VersionReport}
import com.socrata.soda.clients.datacoordinator.{DataCoordinatorClient, RollupDatasetRelation}
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id._
import com.socrata.soda.server.persistence.DatasetRecord
import com.socrata.soda.server.resources.SFCollocateOperation
import com.socrata.soda.server.util.RelationSide.RelationSide
import com.socrata.soda.server.wiremodels.{IndexSpec, RollupSpec, UserProvidedDatasetSpec, UserProvidedIndexSpec, UserProvidedRollupSpec}
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime

trait DatasetDAO {
  import DatasetDAO.Result
  def createDataset(user: String, spec: UserProvidedDatasetSpec): Result
  def replaceOrCreateDataset(user: String,
                             dataset: ResourceName,
                             spec: UserProvidedDatasetSpec): Result
  def updateDataset(user: String,
                    dataset: ResourceName,
                    spec: UserProvidedDatasetSpec): Result
  def markDatasetForDeletion(user: String, dataset: ResourceName, datetime: Option[DateTime]): Result
  def unmarkDatasetForDeletion(user: String, dataset: ResourceName) : Result
  def removeDataset(user: String, dataset: ResourceName, expectedDataVersion: Option[Long]): Result
  def getDataset(dataset: ResourceName, stage: Option[Stage]): Result
  def getSecondaryVersions(dataset: ResourceName): Result
  def getVersion(dataset: ResourceName, secondary: SecondaryId): Result
  def getCurrentCopyNum(dataset: ResourceName): Option[Long]

  def makeCopy(user: String, dataset: ResourceName, expectedDataVersion: Option[Long], copyData: Boolean): Result
  def dropCurrentWorkingCopy(user: String, dataset: ResourceName, expectedDataVersion: Option[Long]): Result
  def publish(user: String, dataset: ResourceName, expectedDataVersion: Option[Long]): Result
  def propagateToSecondary(dataset: ResourceName, secondary: SecondaryId, secondariesLike: Option[ResourceName]): Result
  def deleteFromSecondary(dataset: ResourceName, secondary: SecondaryId): Result

  def replaceOrCreateRollup(user: String,
                            dataset: ResourceName,
                            expectedDataVersion: Option[Long],
                            rollup: RollupName,
                            spec: UserProvidedRollupSpec): Result
  def getRollups(dataset: ResourceName): Result
  def deleteRollups(user: String, dataset: ResourceName, expectedDataVersion: Option[Long],rollups: Seq[RollupName]): Result
  def collocate(secondaryId: SecondaryId, operation: SFCollocateOperation, explain: Boolean, jobId: String): Result
  def collocateStatus(dataset: ResourceName, secondaryId: SecondaryId, jobId: String): Result
  def deleteCollocate(dataset: ResourceName, secondaryId: SecondaryId, jobId: String): Result
  def secondaryReindex(user: String, dataset: ResourceName, expectedDataVersion: Option[Long]): Result

  def replaceOrCreateIndex(user: String,
                           dataset: ResourceName,
                           expectedDataVersion: Option[Long],
                           name: IndexName,
                           spec: UserProvidedIndexSpec): Result
  def getIndexes(dataset: ResourceName): Result

  def getRollupRelations(dataset: ResourceName, relationSide: RelationSide): Result
  def deleteIndexes(user: String, dataset: ResourceName, expectedDataVersion: Option[Long], names: Seq[IndexName]): Result

  def markRollupAccessed(resourceName: ResourceName,rollupName: RollupName):Result
}

object DatasetDAO {
  @JsonKeyStrategy(Strategy.Underscore)
  case class Cost(moves: Int, totalSizeBytes: Long, moveSizeMaxBytes: Option[Long] = None)
  object Cost {
    implicit val codec = AutomaticJsonCodecBuilder[Cost]
    def apply(c: DataCoordinatorClient.Cost): Cost = {
      Cost(c.moves, c.totalSizeBytes, c.moveSizeMaxBytes)
    }
  }
  @JsonKeyStrategy(Strategy.Underscore)
  case class Move(resourceName: ResourceName,
                  storeIdFrom: String,
                  storeIdTo: String,
                  cost: Cost,
                  complete: Option[Boolean] = None)
  object Move {
    implicit val codec = AutomaticJsonCodecBuilder[Move]
    def apply(m: DataCoordinatorClient.Move, translator: DatasetInternalName => Option[ResourceName]): Option[Move] = {
      translator(m.datasetInternalName).map { resourceName =>
        Move(
          resourceName,
          m.storeIdFrom,
          m.storeIdTo,
          Cost(m.cost),
          m.complete
        )
      }
    }
  }

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
  case object Undeleted extends SuccessResult
  case object EmptyResult extends SuccessResult
  case class WorkingCopyCreated(newDataVersion: Long, newDataShapeVersion: Long) extends SuccessResult
  case class WorkingCopyDropped(newDataVersion: Long, newDataShapeVersion: Long) extends SuccessResult
  case class WorkingCopyPublished(newDataVersion: Long, newDataShapeVersion: Long) extends SuccessResult
  case object PropagatedToSecondary extends SuccessResult
  case object DeletedFromSecondary extends SuccessResult
  case class Rollups(rollups: Seq[RollupSpec]) extends SuccessResult

  case class RollupRelations(rollupRelations: Set[RollupDatasetRelation]) extends SuccessResult

  case class RollupMarkedAccessed() extends SuccessResult

  case object RollupCreatedOrUpdated extends SuccessResult
  case object RollupDropped extends SuccessResult
  case class Indexes(indexes: Seq[IndexSpec]) extends SuccessResult
  case object IndexCreatedOrUpdated extends SuccessResult
  case object IndexDropped extends SuccessResult
  case class CollocateDone(jobId : Option[String], status: String, message: String, cost: Cost, moves: Seq[Move]) extends SuccessResult
  object CollocateDone {
    def apply(r: DataCoordinatorClient.CollocateResult, translator: DatasetInternalName => Option[ResourceName]): CollocateDone = {
      CollocateDone(
        r.jobId,
        r.status,
        r.message,
        Cost(r.cost),
        r.moves.flatMap(Move(_, translator)) // NOTE: currently this will not filter out datasets marked as deleted in soda-fountain
      )
    }
  }

  // FAILURES: DataCoordinator
  case class RollupNotFound(name: RollupName) extends FailResult

  case class RollupRelationsNotFound() extends FailResult
  case class IndexNotFound(name: IndexName) extends FailResult
  case class DatasetNotFound(name: ResourceName) extends FailResult
  case class DatasetVersionMismatch(name: ResourceName, version: Long) extends FailResult
  case class CannotAcquireDatasetWriteLock(name: ResourceName) extends FailResult
  case class FeedbackInProgress(name: ResourceName, stores: Set[String]) extends FailResult
  case class IncorrectLifecycleStageResult(actualStage: String, expectedStage: Set[String]) extends FailResult
  case class InternalServerError(code: String, tag: String, data: String) extends FailResult
  case class UnexpectedInternalServerResponse(reason: String, tag: String) extends FailResult
  case class GenericCollocateError(body: String) extends FailResult

  // FAILURES: Internally consumed
  case class InvalidDatasetName(name: ResourceName) extends FailResult
  case class NonExistentColumn(dataset: ResourceName, name: ColumnName) extends FailResult
  case class DatasetAlreadyExists(name: ResourceName) extends FailResult
  case class LocaleChanged(locale: String) extends FailResult
  case class RollupError(message: String) extends FailResult
  case class RollupColumnNotFound(column: ColumnName) extends FailResult
  case class IndexError(message: String) extends FailResult
  case class UnsupportedUpdateOperation(message: String) extends FailResult
}
