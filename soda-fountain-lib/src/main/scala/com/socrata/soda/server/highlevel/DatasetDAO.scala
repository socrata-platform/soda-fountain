package com.socrata.soda.server.highlevel

import com.socrata.soda.server.wiremodels.{DatasetSpec, UserProvidedDatasetSpec}
import com.socrata.soda.server.id.{SecondaryId, ResourceName}
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.VersionReport

trait DatasetDAO {
  import DatasetDAO.Result
  def createDataset(user: String, spec: UserProvidedDatasetSpec): Result
  def replaceOrCreateDataset(user: String, dataset: ResourceName, spec: UserProvidedDatasetSpec): Result
  def updateDataset(user: String, dataset: ResourceName, spec: UserProvidedDatasetSpec): Result
  def deleteDataset(user: String, dataset: ResourceName): Result
  def getDataset(dataset: ResourceName): Result
  def getVersion(dataset: ResourceName, secondary: SecondaryId): Result

  def makeCopy(user: String, dataset: ResourceName, copyData: Boolean): Result
  def dropCurrentWorkingCopy(user: String, dataset: ResourceName): Result
  def publish(user: String, dataset: ResourceName, snapshotLimit: Option[Int]): Result
  def propagateToSecondary(dataset: ResourceName, secondary: SecondaryId): Result
}

object DatasetDAO {
  sealed abstract class Result
  case class Created(datasetSpec: DatasetSpec) extends Result
  case class Updated(datasetSpec: DatasetSpec) extends Result
  case class Found(datasetSpec: DatasetSpec) extends Result
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
}
