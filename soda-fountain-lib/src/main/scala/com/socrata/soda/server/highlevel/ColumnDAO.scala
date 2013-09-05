package com.socrata.soda.server.highlevel

import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.wiremodels.{UserProvidedColumnSpec, ColumnSpec}
import com.socrata.soda.server.id.ResourceName

trait ColumnDAO {
  import ColumnDAO.Result
  def createColumn(dataset: ResourceName, spec: UserProvidedColumnSpec): Result
  def replaceOrCreateColumn(dataset: ResourceName, column: ColumnName, spec: UserProvidedColumnSpec): Result
  def updateColumn(dataset: ResourceName, column: ColumnName, spec: UserProvidedColumnSpec): Result
  def deleteColumn(dataset: ResourceName, column: ColumnName): Result
  def getColumn(dataset: ResourceName, column: ColumnName): Result
}

object ColumnDAO {
  sealed abstract class Result
  case class Created(columnSpec: ColumnSpec) extends Result
  case class Updated(columnSpec: ColumnSpec) extends Result
  case class Found(columnSpec: ColumnSpec) extends Result
  case class NotFound(column: ColumnName) extends Result
  case object Deleted extends Result
  case class InvalidColumnName(name: ColumnName) extends Result
}
