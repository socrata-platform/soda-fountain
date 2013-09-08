package com.socrata.soda.server.persistence

import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soql.environment.ColumnName

// TODO: this needs to expose a notion of transactions
trait NameAndSchemaStore {
  def addResource(newRecord: DatasetRecord)
  def removeResource(resourceName: ResourceName)
  def translateResourceName(resourceName: ResourceName): Option[(DatasetId, Map[ColumnName, ColumnId])]
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
