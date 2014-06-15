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
import org.scalatest.{ShouldMatchers, BeforeAndAfterAll, FunSuite}
import com.socrata.soda.server.persistence.pg.PostgresStoreImpl


class PostgresStoreTest extends FunSuite with ShouldMatchers with BeforeAndAfterAll {
  lazy val config = new SodaFountainConfig(ConfigFactory.load())
  lazy val store = new PostgresStoreImpl(DataSourceFromConfig(config.database))

  override def beforeAll = {
    setupDatabase(config.database.database)
  }

  test("Postgres add/get/remove resourceName and datasetId - no columns") {
    val time = System.currentTimeMillis().toString
    val resourceName = new ResourceName("postgres name store integration test @" + time)
    val datasetId = new DatasetId("postgres.name.store.test @" + time)
    val record = mockDataset(resourceName, datasetId, Seq.empty[ColumnRecord])

    store.addResource(record)
    val foundRecord = store.translateResourceName(resourceName)
    foundRecord match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky, cols, _, _)) =>
        rn should equal (resourceName)
        did should equal (datasetId)
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

    // TODO : Add computation strategy to one of the columns
    val columns = Seq[ColumnRecord](
      new ColumnRecord(
        ColumnId("abc123"),
        ColumnName("a b c 1 2 3"),
        SoQLText,
        "column name human",
        "column desc human",
        false,
        None),
      new ColumnRecord(
        ColumnId("def456"),
        ColumnName("d e f 4 5 6"),
        SoQLText,
        "column name human",
        "column desc human",
        false,
        None)
    )

    val record = mockDataset(resourceName, datasetId, columns)
    store.addResource(record)

    val f = store.translateResourceName(resourceName)
    f match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky, Seq(MinimalColumnRecord(col1, _, _, _), MinimalColumnRecord(col2, _, _, _)), _, _)) =>
        col1 should equal (ColumnId("abc123"))
        col2 should equal (ColumnId("def456"))
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
    val columns = Seq(ColumnRecord(ColumnId("one"), ColumnName("field_name"), SoQLText, "name", "desc",false, None))
    val record = mockDataset(resourceName, datasetId, columns)

    store.addResource(record)
    store.updateColumnFieldName(datasetId, ColumnId("one"), ColumnName("new_field_name"))
    val f = store.translateResourceName(resourceName)
    f match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky,
        Seq(MinimalColumnRecord(columnId, columnName, _, _)), _, _)) =>
        columnId should be (ColumnId("one"))
        columnName should be (ColumnName("new_field_name"))
      case None => fail("didn't find columns")
    }
  }

  private def setupDatabase(dbName: String) {
    synchronized {
      try {
        Class.forName("org.postgresql.Driver").newInstance()
      } catch {
        case ex: ClassNotFoundException => throw ex
      }
      using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "blist", "blist")) {
        conn =>
          conn.setAutoCommit(true)
          val sql = s"drop database if exists $dbName; create database $dbName;"
          using(conn.createStatement()) {
            stmt =>
              stmt.execute(sql)
          }
      }
      using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/$dbName", "blist", "blist")) {
        conn =>
          com.socrata.soda.server.persistence.pg.Migration.migrateDb(conn)
      }
    }
  }

  private def mockDataset(resourceName: ResourceName, datasetId: DatasetId, columns: Seq[ColumnRecord]) = {
    new DatasetRecord(
      resourceName,
      datasetId,
      "human name",
      "human description",
      "locale string",
      "mock schema string",
      new ColumnId("mock column id"),
      columns,
      0,
      new DateTime(0))
  }
}
