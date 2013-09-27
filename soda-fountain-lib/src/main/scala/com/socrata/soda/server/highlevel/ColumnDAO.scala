package com.socrata.soda.server.highlevel

import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.wiremodels.{UserProvidedColumnSpec, ColumnSpec}
import com.socrata.soda.server.id.ResourceName
import com.socrata.http.server.util.{EntityTag, Precondition}

trait ColumnDAO {
  import ColumnDAO.Result
  def replaceOrCreateColumn(user: String, dataset: ResourceName, precondition: Precondition, column: ColumnName, spec: UserProvidedColumnSpec): Result
  def updateColumn(user: String, dataset: ResourceName, column: ColumnName, spec: UserProvidedColumnSpec): Result
  def deleteColumn(user: String, dataset: ResourceName, column: ColumnName): Result
  def getColumn(dataset: ResourceName, column: ColumnName): Result
}

object ColumnDAO {
  sealed abstract class Result
  case class PreconditionFailed(reason: Precondition.Failure) extends Result
  case class Created(columnSpec: ColumnSpec, etag: Option[EntityTag]) extends Result
  case class Updated(columnSpec: ColumnSpec, etag: Option[EntityTag]) extends Result
  case class Found(columnSpec: ColumnSpec, etag: Option[EntityTag]) extends Result
  case class DatasetNotFound(dataset: ResourceName) extends Result
  case class ColumnNotFound(column: ColumnName) extends Result
  case object Deleted extends Result
  case class InvalidColumnName(name: ColumnName) extends Result
}
