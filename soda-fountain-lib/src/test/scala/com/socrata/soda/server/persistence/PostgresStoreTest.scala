package com.socrata.soda.server.persistence

import com.rojoma.simplearm.util._
import com.socrata.soda.server.config.SodaFountainConfig
import com.socrata.soda.server.SodaFountainForTest
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLText
import com.typesafe.config.ConfigFactory
import java.sql.DriverManager
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers


class PostgresStoreTest extends FunSuite with MustMatchers with BeforeAndAfterAll {

  override def beforeAll = {
    val config = new SodaFountainConfig(ConfigFactory.load())
    createDatabase(config.database.database)
  }

  lazy val store : NameAndSchemaStore = SodaFountainForTest.store

  test("Postgress add/get/remove resourceName and datasetId"){
    val time = System.currentTimeMillis().toString
    val resourceName = new ResourceName("postgres name store integration test @" + time)
    val datasetId = new DatasetId("postgres.name.store.test @" + time)
    val record = new DatasetRecord(resourceName, datasetId, "human name", "human description", "locale string", "mock schema string", new ColumnId("mock column id"), Seq.empty[ColumnRecord], 0, new DateTime(0))
    store.addResource(record)
    val foundRecord = store.translateResourceName(resourceName)
    foundRecord match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky, cols, _, _)) =>
        rn must equal (resourceName)
        did must equal (datasetId)
      case None => fail("didn't save or find id")
    }
    store.removeResource(resourceName)
    val f2 = store.translateResourceName(resourceName)
    f2 match {
      case Some(mdr) => fail("resource name should have been removed")
      case None => {}
    }
  }

  test("Postgress add/get/remove columnNames and columnIds"){
    val time = System.currentTimeMillis().toString
    val resourceName = new ResourceName("postgres name store column integration test @" + time)
    val datasetId = new DatasetId("postgres.name.store.column.test @" + time)
    val columns = Seq[ColumnRecord](
      new ColumnRecord(ColumnId("abc123"), ColumnName("a b c 1 2 3"), SoQLText, "column name human", "column desc human",false),
      new ColumnRecord(ColumnId("def456"), ColumnName("d e f 4 5 6"), SoQLText, "column name human", "column desc human",false)
    )
    val record = new DatasetRecord(resourceName, datasetId, "human name", "human description", "locale string", "mock schema string", new ColumnId("mock column id"), columns, 0, new DateTime(0))
    store.addResource(record)
    val f = store.translateResourceName(resourceName)
    f match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky, Seq(MinimalColumnRecord(col1, _, _, _), MinimalColumnRecord(col2, _, _, _)), _, _)) =>
        col1 must equal (ColumnId("abc123"))
        col2 must equal (ColumnId("def456"))
      case None => fail("didn't find columns")
    }
    store.removeResource(resourceName)
    val f2 = store.translateResourceName(resourceName)
    f2 match {
      case Some(mdr) => fail("resource name should have been removed")
      case None => {}
    }
  }

  test("Postgres rename field name"){
    val time = System.currentTimeMillis().toString
    val resourceName = new ResourceName("postgres name store column integration test @" + time)
    val datasetId = new DatasetId("postgres.name.store.column.test @" + time)
    val columns = Seq(ColumnRecord(ColumnId("one"), ColumnName("field_name"), SoQLText, "name", "desc",false))
    val record = new DatasetRecord(resourceName, datasetId, "human name", "human description", "locale string", "mock schema string", new ColumnId("mock column id"), columns, 0, new DateTime(0))
    store.addResource(record)
    store.updateColumnFieldName(datasetId, ColumnId("one"), ColumnName("new_field_name"))
    val f = store.translateResourceName(resourceName)
    f match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky,
        Seq(MinimalColumnRecord(columnId, columnName, _, _)), _, _)) =>
        columnId must be (ColumnId("one"))
        columnName must be (ColumnName("new_field_name"))
      case None => fail("didn't find columns")
    }
  }

  private def createDatabase(dbName: String) {
    synchronized {
      try {
        Class.forName("org.postgresql.Driver").newInstance()
      } catch {
        case ex: ClassNotFoundException => throw ex
      }
      println(s"creating database $dbName")
      using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "blist", "blist")) { conn =>
        conn.setAutoCommit(true)
        val sql = s"drop database if exists $dbName; create database $dbName;"
        using(conn.createStatement()) { stmt =>
          stmt.execute(sql)
          println(s"database $dbName created")
        }
      }
    }
  }
}
