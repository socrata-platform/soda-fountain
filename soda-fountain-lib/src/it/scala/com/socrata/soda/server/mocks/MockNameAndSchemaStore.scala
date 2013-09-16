package com.socrata.soda.server.mocks

import com.socrata.soda.server.services.SodaService
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.soda.server.types.{ColumnId, DatasetId, ResourceName}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType
import scala.util.Try
import scala.collection.mutable

trait MockNameAndSchemaStore extends SodaService {
  val store: NameAndSchemaStore = mock

  private object mock extends NameAndSchemaStore {
    val names = new scala.collection.mutable.HashMap[ResourceName, DatasetId]
    val columns = new mutable.HashMap[DatasetId, Map[ColumnName, ColumnId]]

    def addResource(resourceName: ResourceName, datasetId: DatasetId, columnNames: Map[ColumnName, ColumnId]): Try[Unit] = Try { names.put(resourceName, datasetId)}
    def removeResource(resourceName: ResourceName): Try[Unit] = Try{ names.remove(resourceName) }
    def translateResourceName( resourceName: ResourceName): Option[(DatasetId, Map[ColumnName, ColumnId])] = {
      for {
        id <- names.get(resourceName)
        columns <- columns.get(id)
      } yield (id, columns)
    }

    def addColumn(datasetId: DatasetId, columnSystemId: ColumnId, columnFieldName: ColumnName) : Try[Unit] = ???
    def updateColumnFieldName(datasetId: DatasetId, columnId: ColumnName, newFieldName: ColumnName) : Try[Unit] = ???
    def dropColumn(datasetId: DatasetId, columnId: ColumnId) : Try[Unit] = ???
  }
}
