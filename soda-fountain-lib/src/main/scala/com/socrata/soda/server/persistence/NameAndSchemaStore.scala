package com.socrata.soda.server.persistence

import scala.{collection => sc}
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.wiremodels.DatasetSpec

// TODO: this needs to expose a notion of transactions
trait NameAndSchemaStore {
  def addResource(datasetId: DatasetId, datasetSpec: DatasetSpec)
  def removeResource(resourceName: ResourceName)
  def translateResourceName(resourceName: ResourceName): Option[(DatasetId, sc.Map[ColumnName, ColumnId])]
  def lookupDataset(resourceName: ResourceName): Option[DatasetRecord]

  def addColumn(datasetId: DatasetId, columnSystemId: ColumnId, columnFieldName: ColumnName) : Unit
  def updateColumnFieldName(datasetId: DatasetId, columnId: ColumnName, newFieldName: ColumnName) : Unit
  def dropColumn(datasetId: DatasetId, columnId: ColumnId) : Unit
}

case class DatasetRecord(resourceName: ResourceName, systemId: DatasetId, name: String, description: String, columns: Seq[ColumnRecord]) {
  lazy val columnsByName = columns.groupBy(_.fieldName).mapValues(_.head)
  lazy val columnsById = columns.groupBy(_.id).mapValues(_.head)
}
case class ColumnRecord(id: ColumnId, fieldName: ColumnName, name: String, description: String)
