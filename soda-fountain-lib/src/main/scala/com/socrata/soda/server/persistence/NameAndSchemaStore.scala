package com.socrata.soda.server.persistence

import scala.{collection => sc}
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soql.environment.ColumnName
import scala.util.Try
import com.socrata.soda.server.wiremodels.DatasetSpec

// TODO: this needs to expose a notion of transactions
trait NameAndSchemaStore {
  def addResource(datasetId: DatasetId, datasetSpec: DatasetSpec): Try[Unit]
  def removeResource(resourceName: ResourceName): Try[Unit]
  def translateResourceName( resourceName: ResourceName): Option[(DatasetId, sc.Map[ColumnName, ColumnId])]

  def addColumn(datasetId: DatasetId, columnSystemId: ColumnId, columnFieldName: ColumnName) : Try[Unit]
  def updateColumnFieldName(datasetId: DatasetId, columnId: ColumnName, newFieldName: ColumnName) : Try[Unit]
  def dropColumn(datasetId: DatasetId, columnId: ColumnId) : Try[Unit]
}
