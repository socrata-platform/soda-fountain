package com.socrata.soda.server.persistence

import javax.sql.DataSource
import com.rojoma.simplearm.util._
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import scala.{collection => sc}
import com.socrata.soql.environment.ColumnName
import scala.util.Try

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
      using(connection.prepareStatement("select dataset_system_id from datasets where resource_name = ?")){ translator =>
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

  def addResource(resourceName: ResourceName, datasetId: DatasetId, columnNames: Map[ColumnName, ColumnId])  : Try[Unit] = {
    Try {
      using(dataSource.getConnection()){ connection =>
        using(connection.prepareStatement("insert into datasets (resource_name, dataset_system_id) values(?, ?)")){ adder =>
          adder.setString(1, resourceName.caseFolded)
          adder.setString(2, datasetId.underlying)
          adder.execute()
        }
      }
    }
  }

  def removeResource(resourceName: ResourceName) : Try[Unit] = {
    Try {
      using(dataSource.getConnection()){ connection =>
        using(connection.prepareStatement("delete from datasets where resource_name = ?")){ deleter =>
          deleter.setString(1, resourceName.toString)
          deleter.execute()
        }
      }
    }
  }

  def addColumn(datasetId: DatasetId, columnSystemId: ColumnId, columnFieldName: ColumnName) : Try[Unit] = ???
  def updateColumnFieldName(datasetId: DatasetId, columnId: ColumnName, newFieldName: ColumnName) : Try[Unit] = ???
  def dropColumn(datasetId: DatasetId, columnId: ColumnId) : Try[Unit] = ???
}
