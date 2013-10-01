package com.socrata.soda.server.persistence

import com.socrata.soda.server.config.DataSourceConfig
import javax.sql.DataSource
import org.postgresql.ds.PGSimpleDataSource
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.mchange.v2.c3p0.DataSources

object DataSourceFromConfig {
  /**
   * The return value from this function may or may not be a c3p0 data source.  It should be
   * destroyed (by c3p0's `DataSources.destroy` method) when you're done with that, as that
   * will do the right thing with non-c3p0 sources.
   */
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
