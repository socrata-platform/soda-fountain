package com.socrata.soda.server.persistence

import com.rojoma.simplearm.v2._
import com.socrata.soda.server.config.SodaFountainConfig
import com.socrata.soda.server.persistence.pg.PostgresStoreImpl
import com.typesafe.config.ConfigFactory
import java.sql.DriverManager
import org.scalatest.{FunSuite, BeforeAndAfterAll}

trait SodaFountainDatabaseTest extends FunSuite with BeforeAndAfterAll {
  lazy val config = new SodaFountainConfig(ConfigFactory.load())
  lazy val dataSource = DataSourceFromConfig(config.database)
  lazy val store = new PostgresStoreImpl(dataSource)

  override def beforeAll = {
    setupDatabase(config.database.database)
  }

  private def setupDatabase(dbName: String) {
    synchronized {
      try {
        Class.forName("org.postgresql.Driver").newInstance()
      } catch {
        case ex: ClassNotFoundException => throw ex
      }
      using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "blist", "blist")) { conn =>
        conn.setAutoCommit(true)
        val sql = s"drop database if exists $dbName; create database $dbName;"
        using(conn.createStatement()) { stmt =>
          stmt.execute(sql)
        }
      }
      using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/$dbName", "blist", "blist")) { conn =>
        com.socrata.soda.server.persistence.pg.Migration.migrateDb(conn)
      }
    }
  }
}
