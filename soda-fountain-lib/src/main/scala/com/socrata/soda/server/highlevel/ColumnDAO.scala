package com.socrata.soda.server.highlevel

import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.persistence.{DatasetRecord, ColumnRecord}
import com.socrata.soda.server.wiremodels.UserProvidedColumnSpec
import com.socrata.soql.environment.ColumnName

trait ColumnDAO {
  import ColumnDAO.Result
  def replaceOrCreateColumn(user: String, dataset: ResourceName, precondition: Precondition, column: ColumnName, spec: UserProvidedColumnSpec): Result
  def updateColumn(user: String, dataset: ResourceName, column: ColumnName, spec: UserProvidedColumnSpec): Result
  def deleteColumn(user: String, dataset: ResourceName, column: ColumnName): Result
  def makePK(user: String, dataset: ResourceName, column: ColumnName): Result
  def getColumn(dataset: ResourceName, column: ColumnName): Result
}

object ColumnDAO {
  sealed trait Result
  sealed trait CreateUpdateSuccess extends Result
  case class Created(columnRec: ColumnRecord, etag: Option[EntityTag]) extends CreateUpdateSuccess
  case class Updated(columnRec: ColumnRecord, etag: Option[EntityTag]) extends CreateUpdateSuccess
  case class PreconditionFailed(reason: Precondition.Failure) extends Result
  case class Found(datasetRec: DatasetRecord, columnRec: ColumnRecord, etag: Option[EntityTag]) extends Result
  case class DatasetNotFound(dataset: ResourceName) extends Result
  case class ColumnNotFound(column: ColumnName) extends Result
  case class Deleted(rec: ColumnRecord, etag: Option[EntityTag]) extends Result
  case class InvalidColumnName(name: ColumnName) extends Result
  case class InvalidRowIdOperation(columnRec: ColumnRecord, method: String) extends Result
  case class NonUniqueRowId(rec: ColumnRecord) extends Result
}
