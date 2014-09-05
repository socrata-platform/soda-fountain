package com.socrata.soda.server.highlevel

import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.VersionReport
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.{RollupName, SecondaryId, ResourceName}
import com.socrata.soda.server.persistence.DatasetRecord
import com.socrata.soda.server.wiremodels.{UserProvidedRollupSpec, UserProvidedDatasetSpec}
import com.socrata.soql.environment.ColumnName

trait DatasetDAO {
  import DatasetDAO.Result
  def createDataset(user: String, spec: UserProvidedDatasetSpec): Result
  def replaceOrCreateDataset(user: String, dataset: ResourceName, spec: UserProvidedDatasetSpec): Result
  def updateDataset(user: String, dataset: ResourceName, spec: UserProvidedDatasetSpec): Result
  def deleteDataset(user: String, dataset: ResourceName): Result
  def getDataset(dataset: ResourceName, stage: Option[Stage]): Result
  def getVersion(dataset: ResourceName, secondary: SecondaryId): Result

  def makeCopy(user: String, dataset: ResourceName, copyData: Boolean): Result
  def dropCurrentWorkingCopy(user: String, dataset: ResourceName): Result
  def publish(user: String, dataset: ResourceName, snapshotLimit: Option[Int]): Result
  def propagateToSecondary(dataset: ResourceName, secondary: SecondaryId): Result

  def replaceOrCreateRollup(user: String, dataset: ResourceName, rollup: RollupName, spec: UserProvidedRollupSpec): Result
  def getRollup(user: String, dataset: ResourceName, rollup: RollupName): Result
  def deleteRollup(user: String, dataset: ResourceName, rollup: RollupName): Result
}

object DatasetDAO {
  sealed abstract class Result
  case class Created(datasetSpec: DatasetRecord) extends Result
  case class Updated(datasetSpec: DatasetRecord) extends Result
  case class Found(datasetSpec: DatasetRecord) extends Result
  case class DatasetVersion(version: VersionReport) extends Result
  case object Deleted extends Result
  case class NotFound(name: ResourceName) extends Result
  case class InvalidDatasetName(name: ResourceName) extends Result
  case class NonexistantColumn(name: ColumnName) extends Result
  case class InvalidColumnName(name: ColumnName) extends Result
  case class DatasetAlreadyExists(name: ResourceName) extends Result
  case object LocaleChanged extends Result
  case object WorkingCopyCreated extends Result
  case object WorkingCopyDropped extends Result
  case object WorkingCopyPublished extends Result
  case object PropagatedToSecondary extends Result
  case object RollupCreatedOrUpdated extends Result
  case object RollupDropped extends Result
  case class RollupError(message: String) extends Result
  case class RollupColumnNotFound(column: ColumnName) extends Result
  case class RollupNotFound(name: RollupName) extends Result
}
