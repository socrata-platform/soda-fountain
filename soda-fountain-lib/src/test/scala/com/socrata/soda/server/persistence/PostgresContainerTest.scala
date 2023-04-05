package com.socrata.soda.server.persistence

import org.postgresql.ds.PGSimpleDataSource
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.testcontainers.containers.PostgreSQLContainer

import java.sql.{Connection, DriverManager}
import javax.sql.DataSource

trait PostgresContainerTest extends FunSuite with BeforeAndAfterAll {
  private lazy val postgresContainer: PostgreSQLContainer[Nothing] = {
    new PostgreSQLContainer("postgres:15.1")
  }
  protected lazy val postgresConnection: Connection = {
    postgresContainer.start()
    DriverManager.getConnection(postgresContainer.getJdbcUrl, postgresContainer.getUsername, postgresContainer.getPassword)
  }

  protected lazy val postgresDatasource:DataSource ={
    val pgDataSource = new PGSimpleDataSource();
    pgDataSource.setUrl(postgresContainer.getJdbcUrl)
    pgDataSource.setUser(postgresContainer.getUsername)
    pgDataSource.setPassword(postgresContainer.getPassword)
    pgDataSource
  }

  override protected def afterAll(): Unit = {
    postgresConnection.close()
    postgresContainer.close()
  }
}
