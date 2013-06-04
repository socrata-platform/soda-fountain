package com.socrata.soda.server.persistence

import com.socrata.soda.server.services.SodaService
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.config.Config
import javax.sql.DataSource
import org.postgresql.ds.PGSimpleDataSource

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

  val store: NameAndSchemaStore = postgres
  val dataSource = DataSourceFromConfig(new DataSourceConfig(SodaService.config.getConfig("database")))

  object postgres extends NameAndSchemaStore {
    def translateResourceName( resourceName: String) : Future[Either[String, String]] = {
      future {
        ???
      }
    }
    def add(resourceName: String, datasetId: String) = {
      ???
    }
    def remove(resourceName: String) = {
      ???
    }
  }
}


