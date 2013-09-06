package com.socrata.soda.server.persistence

import javax.sql.DataSource
import com.rojoma.simplearm.util._
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import scala.{collection => sc}
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.wiremodels.DatasetSpec
import java.sql.Connection

class PostgresStoreImpl(dataSource: DataSource) extends NameAndSchemaStore {
  using(dataSource.getConnection()){ connection =>
    using(connection.createStatement()){ stmt =>
      using(getClass.getClassLoader.getResourceAsStream("db_create.sql")){ stream =>
        val createScript = scala.io.Source.fromInputStream(stream, "UTF-8").getLines().mkString("\n")
        stmt.execute(createScript)
      }
    }
  }

  def translateResourceName( resourceName: ResourceName): Option[(DatasetId, sc.Map[ColumnName, ColumnId])] = {
    using(dataSource.getConnection()){ connection =>
      using(connection.prepareStatement("select dataset_system_id from datasets where resource_name_casefolded = ?")){ translator =>
        translator.setString(1, resourceName.caseFolded)
        val rs = translator.executeQuery()
        rs.next match {
          case true =>
            val datasetId = rs.getString(1)
            using(connection.prepareStatement("select column_name, column_id from columns where dataset_system_id = ?")){ translator =>
              translator.setString(1, datasetId)
              val columnRS = translator.executeQuery()
              val columns = new scala.collection.mutable.HashMap[ColumnName, ColumnId]
              while (columnRS.next()){
                columns.put(ColumnName(columnRS.getString(1)), ColumnId(columnRS.getString(2)))
              }
              Some((DatasetId(datasetId), columns))
            }
          case false => None
        }
      }
    }
  }

  def lookupDataset(resourceName: ResourceName): Option[DatasetRecord] =
    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(false)
      using(conn.prepareStatement("select resource_name, dataset_system_id, name, description from datasets where resource_name_casefolded = ?")) { dsQuery =>
        dsQuery.setString(1, resourceName.caseFolded)
        using(dsQuery.executeQuery()) { dsResult =>
          if(dsResult.next()) {
            val datasetId = DatasetId(dsResult.getString("dataset_system_id"))
            Some(DatasetRecord(
              new ResourceName(dsResult.getString("resource_name")),
              datasetId,
              dsResult.getString("name"),
              dsResult.getString("description"),
              fetchColumns(conn, datasetId)))
          } else {
            None
          }
        }
      }
    }

  def fetchColumns(conn: Connection, datasetId: DatasetId): Seq[ColumnRecord] = {
    using(conn.prepareStatement("select column_name, column_id, name, description from columns where dataset_system_id = ?")) { colQuery =>
      colQuery.setString(1, datasetId.underlying)
      using(colQuery.executeQuery()) { rs =>
        val result = Vector.newBuilder[ColumnRecord]
        while(rs.next()) {
          result += ColumnRecord(
            ColumnId(rs.getString("column_id")),
            new ColumnName(rs.getString("column_name")),
            rs.getString("name"),
            rs.getString("description")
          )
        }
        result.result()
      }
    }
  }

  def addResource(datasetId: DatasetId, datasetSpec: DatasetSpec): Unit =
    using(dataSource.getConnection()){ connection =>
      connection.setAutoCommit(false)
      using(connection.prepareStatement("insert into datasets (resource_name_casefolded, resource_name, dataset_system_id, name, description) values(?, ?, ?, ?, ?)")){ adder =>
        adder.setString(1, datasetSpec.resourceName.caseFolded)
        adder.setString(2, datasetSpec.resourceName.name)
        adder.setString(3, datasetId.underlying)
        adder.setString(4, datasetSpec.name)
        adder.setString(5, datasetSpec.description)
        adder.execute()
      }
      using(connection.prepareStatement("insert into columns (dataset_system_id, column_name_casefolded, column_name, column_id, name, description) values (?, ?, ?, ?, ?, ?)")) { colAdder =>
        for(cspec <- datasetSpec.columns.values) {
          colAdder.setString(1, datasetId.underlying)
          colAdder.setString(2, cspec.fieldName.caseFolded)
          colAdder.setString(3, cspec.fieldName.name)
          colAdder.setString(4, cspec.id.underlying)
          colAdder.setString(5, cspec.name)
          colAdder.setString(6, cspec.description)
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
