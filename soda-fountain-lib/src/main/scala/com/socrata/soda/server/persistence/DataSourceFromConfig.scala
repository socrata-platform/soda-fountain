package com.socrata.soda.server.persistence

import com.socrata.soda.server.config.DataSourceConfig
import javax.sql.DataSource
import org.postgresql.ds.PGSimpleDataSource

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
