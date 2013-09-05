package com.socrata.soda.server.highlevel

import com.socrata.soda.server.wiremodels.{DatasetSpec, UserProvidedDatasetSpec}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soql.environment.ColumnName

trait DatasetDAO {
  import DatasetDAO.Result
  def createDataset(spec: UserProvidedDatasetSpec): Result
  def replaceOrCreateDataset(dataset: ResourceName, spec: UserProvidedDatasetSpec): Result
  def updateDataset(dataset: ResourceName, spec: UserProvidedDatasetSpec): Result
  def deleteDataset(dataset: ResourceName): Result
  def getDataset(dataset: ResourceName): Result
}

object DatasetDAO {
  sealed abstract class Result
  case class Created(datasetSpec: DatasetSpec) extends Result
  case class Updated(datasetSpec: DatasetSpec) extends Result
  case class Found(datasetSpec: DatasetSpec) extends Result
  case object Deleted extends Result
  case class NotFound(name: ResourceName) extends Result
  case class InvalidDatasetName(name: ResourceName) extends Result
  case class InvalidColumnName(name: ColumnName) extends Result
}
