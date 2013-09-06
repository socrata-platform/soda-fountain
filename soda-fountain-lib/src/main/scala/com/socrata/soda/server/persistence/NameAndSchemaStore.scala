package com.socrata.soda.server.persistence

import scala.{collection => sc}
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soql.environment.ColumnName
import scala.util.Try

// TODO: this needs to expose a notion of transactions
trait NameAndSchemaStore {
  def addResource(resourceName: ResourceName, datasetId: DatasetId, columnNames: Map[ColumnName, ColumnId]): Try[Unit]
  def removeResource(resourceName: ResourceName): Try[Unit]
  def translateResourceName( resourceName: ResourceName): Option[(DatasetId, sc.Map[ColumnName, ColumnId])]

  def addColumn(datasetId: DatasetId, columnSystemId: ColumnId, columnFieldName: ColumnName) : Try[Unit]
  def updateColumnFieldName(datasetId: DatasetId, columnId: ColumnName, newFieldName: ColumnName) : Try[Unit]
  def dropColumn(datasetId: DatasetId, columnId: ColumnId) : Try[Unit]
}
