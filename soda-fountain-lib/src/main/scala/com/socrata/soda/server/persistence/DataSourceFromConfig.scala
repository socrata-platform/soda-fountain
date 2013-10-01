package com.socrata.soda.server.persistence

import com.socrata.soda.server.config.DataSourceConfig
import javax.sql.DataSource
import org.postgresql.ds.PGSimpleDataSource
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.mchange.v2.c3p0.DataSources

object DataSourceFromConfig {
  def apply(config: DataSourceConfig): DataSource = {
    val dataSource = new PGSimpleDataSource
    dataSource.setServerName(config.host)
    dataSource.setPortNumber(config.port)
    dataSource.setDatabaseName(config.database)
    dataSource.setUser(config.username)
    dataSource.setPassword(config.password)
    dataSource.setApplicationName(config.applicationName)
    config.poolOptions match {
      case Some(poolOptions) =>
        val overrideProps = Propertizer("", poolOptions)
        DataSources.pooledDataSource(dataSource, null, overrideProps)
      case None =>
        dataSource
    }
  }
}
