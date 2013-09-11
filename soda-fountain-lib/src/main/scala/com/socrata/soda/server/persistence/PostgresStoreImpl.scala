package com.socrata.soda.server.persistence

import javax.sql.DataSource
import com.rojoma.simplearm.util._
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soql.environment.{TypeName, ColumnName}
import java.sql.Connection
import com.socrata.soql.types.SoQLType

class PostgresStoreImpl(dataSource: DataSource) extends NameAndSchemaStore {
  using(dataSource.getConnection()){ connection =>
    using(connection.createStatement()){ stmt =>
      using(getClass.getClassLoader.getResourceAsStream("db_create.sql")){ stream =>
        val createScript = scala.io.Source.fromInputStream(stream, "UTF-8").getLines().mkString("\n")
        stmt.execute(createScript)
      }
    }
  }

  def translateResourceName(resourceName: ResourceName): Option[MinimalDatasetRecord] = {
    using(dataSource.getConnection()){ connection =>
      using(connection.prepareStatement("select resource_name, dataset_system_id, locale, schema_hash, primary_key_column_id from datasets where resource_name_casefolded = ?")){ translator =>
        translator.setString(1, resourceName.caseFolded)
        val rs = translator.executeQuery()
        if(rs.next()) {
          val datasetId = DatasetId(rs.getString("dataset_system_id"))
          Some(MinimalDatasetRecord(
            new ResourceName(rs.getString("resource_name")),
            datasetId,
            rs.getString("locale"),
            rs.getString("schema_hash"),
            ColumnId(rs.getString("primary_key_column_id")),
            fetchMinimalColumns(connection, datasetId)))
        } else {
          None
        }
      }
    }
  }

  def lookupDataset(resourceName: ResourceName): Option[DatasetRecord] =
    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(false)
      using(conn.prepareStatement("select resource_name, dataset_system_id, name, description, locale, schema_hash, primary_key_column_id, from datasets where resource_name_casefolded = ?")) { dsQuery =>
        dsQuery.setString(1, resourceName.caseFolded)
        using(dsQuery.executeQuery()) { dsResult =>
          if(dsResult.next()) {
            val datasetId = DatasetId(dsResult.getString("dataset_system_id"))
            Some(DatasetRecord(
              new ResourceName(dsResult.getString("resource_name")),
              datasetId,
              dsResult.getString("name"),
              dsResult.getString("description"),
              dsResult.getString("locale"),
              dsResult.getString("schema_hash"),
              ColumnId(dsResult.getString("primary_key_column_id")),
              fetchFullColumns(conn, datasetId)))
          } else {
            None
          }
        }
      }
    }

  def fetchMinimalColumns(conn: Connection, datasetId: DatasetId): Seq[MinimalColumnRecord] = {
    using(conn.prepareStatement("select column_name, column_id, type_name from columns where dataset_system_id = ?")) { colQuery =>
      colQuery.setString(1, datasetId.underlying)
      using(colQuery.executeQuery()) { rs =>
        val result = Vector.newBuilder[MinimalColumnRecord]
        while(rs.next()) {
          result += MinimalColumnRecord(
            ColumnId(rs.getString("column_id")),
            new ColumnName(rs.getString("column_name")),
            SoQLType.typesByName(TypeName(rs.getString("type_name")))
          )
        }
        result.result()
      }
    }
  }

  def fetchFullColumns(conn: Connection, datasetId: DatasetId): Seq[ColumnRecord] = {
    using(conn.prepareStatement("select column_name, column_id, type_name, name, description from columns where dataset_system_id = ?")) { colQuery =>
      colQuery.setString(1, datasetId.underlying)
      using(colQuery.executeQuery()) { rs =>
        val result = Vector.newBuilder[ColumnRecord]
        while(rs.next()) {
          result += ColumnRecord(
            ColumnId(rs.getString("column_id")),
            new ColumnName(rs.getString("column_name")),
            SoQLType.typesByName(TypeName(rs.getString("type_name"))),
            rs.getString("name"),
            rs.getString("description")
          )
        }
        result.result()
      }
    }
  }

  def addResource(newRecord: DatasetRecord): Unit =
    using(dataSource.getConnection()){ connection =>
      connection.setAutoCommit(false)
      using(connection.prepareStatement("insert into datasets (resource_name_casefolded, resource_name, dataset_system_id, name, description, locale, schema_hash, primary_key_column_id) values(?, ?, ?, ?, ?, ?, ?, ?)")){ adder =>
        adder.setString(1, newRecord.resourceName.caseFolded)
        adder.setString(2, newRecord.resourceName.name)
        adder.setString(3, newRecord.systemId.underlying)
        adder.setString(4, newRecord.name)
        adder.setString(5, newRecord.description)
        adder.setString(6, newRecord.locale)
        adder.setString(7, newRecord.schemaHash)
        adder.setString(8, newRecord.primaryKey.underlying)
        adder.execute()
      }
      using(connection.prepareStatement("insert into columns (dataset_system_id, column_name_casefolded, column_name, column_id, name, description, type_name) values (?, ?, ?, ?, ?, ?, ?)")) { colAdder =>
        for(cspec <- newRecord.columns) {
          colAdder.setString(1, newRecord.systemId.underlying)
          colAdder.setString(2, cspec.fieldName.caseFolded)
          colAdder.setString(3, cspec.fieldName.name)
          colAdder.setString(4, cspec.id.underlying)
          colAdder.setString(5, cspec.name)
          colAdder.setString(6, cspec.description)
          colAdder.setString(7, cspec.typ.name.name)
          colAdder.addBatch()
        }
        colAdder.executeBatch()
      }
      connection.commit()
    }

  def removeResource(resourceName: ResourceName): Unit =
    using(dataSource.getConnection()){ connection =>
      connection.setAutoCommit(false)
      val datasetIdOpt = using(connection.prepareStatement("select dataset_system_id from datasets where resource_name_casefolded = ? for update")) { idFetcher =>
        idFetcher.setString(1, resourceName.caseFolded)
        using(idFetcher.executeQuery()) { rs =>
          if(rs.next()) Some(DatasetId(rs.getString(1)))
          else None
        }
      }
      for(datasetId <- datasetIdOpt) {
        using(connection.prepareStatement("delete from columns where dataset_system_id = ?")) { deleter =>
          deleter.setString(1, datasetId.underlying)
          deleter.execute()
        }
        using(connection.prepareStatement("delete from datasets where dataset_system_id = ?")) { deleter =>
          deleter.setString(1, datasetId.underlying)
          deleter.execute()
        }
      }
      connection.commit()
    }

  def addColumn(datasetId: DatasetId, columnSystemId: ColumnId, columnFieldName: ColumnName): Unit = ???
  def updateColumnFieldName(datasetId: DatasetId, columnId: ColumnName, newFieldName: ColumnName) : Unit = ???
  def dropColumn(datasetId: DatasetId, columnId: ColumnId) : Unit = ???
}
