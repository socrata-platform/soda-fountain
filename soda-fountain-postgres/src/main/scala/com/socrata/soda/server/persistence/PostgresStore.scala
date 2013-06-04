package com.socrata.soda.server.persistence

import com.socrata.soda.server.services.SodaService
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.config.Config
import javax.sql.DataSource
import org.postgresql.ds.PGSimpleDataSource
import com.rojoma.simplearm.util._
import java.io.Closeable
import java.sql.{PreparedStatement, Connection}

class DataSourceConfig(config: Config) {
  val host = config.getString("host")
  val port = config.getInt("port")
  val database = config.getString("database")
  val username = config.getString("username")
  val password = config.getString("password")
}

object DataSourceFromConfig {
  def apply(config: DataSourceConfig): DataSource = {
    val dataSource = new PGSimpleDataSource
    dataSource.setServerName(config.host)
    dataSource.setPortNumber(config.port)
    dataSource.setDatabaseName(config.database)
    dataSource.setUser(config.username)
    dataSource.setPassword(config.password)
    dataSource
  }
}

trait PostgresStore extends SodaService {
  val dataSource = DataSourceFromConfig(new DataSourceConfig(SodaService.config.getConfig("database")))
  val store: NameAndSchemaStore = new PostgresStoreImpl(dataSource)
}

class PostgresStoreImpl(dataSource: DataSource) extends NameAndSchemaStore {

  using(dataSource.getConnection()){ connection =>
    using(connection.createStatement()){ stmt =>
      using(getClass.getClassLoader.getResourceAsStream("db_create.sql")){ stream =>
        val createScript = scala.io.Source.fromInputStream(stream, "UTF-8").getLines().mkString("\n")
        stmt.execute(createScript)
      }
    }
  }

  def translateResourceName( resourceName: String) : Future[Either[String, String]] = {
    future {
      using(dataSource.getConnection()){ connection =>
        using(connection.prepareStatement("select dataset_system_id from datasets where resource_name = ?")){ translator =>
          translator.setString(1, resourceName)
          val rs = translator.executeQuery()
          rs.next match {
            case true => Right(rs.getString(1))
            case false => Left("not found")
          }
        }
      }
    }
  }
  def add(resourceName: String, datasetId: String) = {
    future {
      using(dataSource.getConnection()){ connection =>
        using(connection.prepareStatement("insert into datasets (resource_name, dataset_system_id) values(?, ?)")){ adder =>
          adder.setString(1, resourceName)
          adder.setString(2, datasetId)
          adder.execute()
        }
      }
    }
  }
  def remove(resourceName: String) = {
    future {
      using(dataSource.getConnection()){ connection =>
        using(connection.prepareStatement("delete from datasets where resource_name = ?")){ deleter =>
          deleter.setString(1, resourceName)
          deleter.execute()
        }
      }
    }
  }
}


